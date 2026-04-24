"""CLI interface for dbt Job Generator.

Uses click for command-line argument parsing.
Commands: generate, batch, validate, validate-batch.

Test file generation is mandatory — always produces test_{model}_spark.py
alongside the .sql model. Review report is generated as review_report.md
when running batch.
"""

from __future__ import annotations

import os
import sys

import click

from dbt_job_generator.models.errors import ParseError
from dbt_job_generator.models.validation import ProjectContext
from dbt_job_generator.pipeline import Pipeline
from dbt_job_generator.validator.mapping_validator import MappingValidator


def _write_review_report(report, output_dir: str) -> str:
    """Write review report as Markdown file. Returns the file path."""
    lines = ["# Review Report\n"]
    s = report.summary
    lines.append(f"Total: {s.total}, Success: {s.success_count}, "
                 f"Errors: {s.error_count}, Warnings: {s.warning_count}\n")

    if report.execution_order:
        lines.append("\n## Thứ tự thực thi\n")
        for i, name in enumerate(report.execution_order, 1):
            lines.append(f"{i}. {name}")
        lines.append("")

    if report.model_details:
        lines.append("\n## Chi tiết Models\n")
        lines.append("| Model | Patterns | Complex | Exception | CTEs | Test Status |")
        lines.append("|-------|----------|---------|-----------|------|-------------|")
        for d in report.model_details:
            patterns = ", ".join(d.patterns_used) if d.patterns_used else "-"
            lines.append(
                f"| {d.model_name} | {patterns} | "
                f"{'Yes' if d.has_complex_logic else 'No'} | "
                f"{'Yes' if d.has_exceptions else 'No'} | "
                f"{d.cte_count} | {d.test_compliance_status or '-'} |"
            )
        lines.append("")

    if report.attention_items:
        lines.append("\n## Điểm cần chú ý\n")
        for item in report.attention_items:
            lines.append(f"- **{item.model_name}**: {item.reason}")
            if item.details:
                lines.append(f"  - {item.details}")
        lines.append("")

    report_path = os.path.join(output_dir, "review_report.md")
    os.makedirs(output_dir, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return report_path


@click.group()
def main():
    """dbt Job Generator — Auto-generate dbt models from mapping CSV files."""
    pass


@main.command()
@click.argument("file_path")
@click.option("--output", "-o", default=None, help="Output file path")
@click.option("--schema-dir", default=None, type=click.Path(exists=True),
              help="Path to schema directory for schema validation")
@click.option("--test-output-dir", default=None,
              help="Directory for Spark test files (default: alongside output)")
@click.option("--rules-csv", default=None, type=click.Path(exists=True),
              help="Path to transformation rules CSV file")
def generate(file_path: str, output: str | None, schema_dir: str | None,
             test_output_dir: str | None, rules_csv: str | None):
    """Generate a single dbt model + Spark test from a mapping CSV file."""
    from dbt_job_generator.catalog.rule_catalog import TransformationRuleCatalog

    effective_schema_dir = schema_dir or "schemas"
    catalog = TransformationRuleCatalog.load_from_csv(rules_csv) if rules_csv else None
    pipeline = Pipeline(schema_dir=effective_schema_dir, catalog=catalog)

    try:
        sql_content = pipeline.generate_model(file_path, schema_dir=effective_schema_dir)
    except ParseError as e:
        click.echo(f"Parse error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Generation error: {e}", err=True)
        sys.exit(1)

    if output:
        os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
        with open(output, "w", encoding="utf-8") as f:
            f.write(sql_content)
        click.echo(f"Generated: {output}")
    else:
        click.echo(sql_content)

    # Always generate Spark test file
    try:
        from dbt_job_generator.spark_test_gen import copy_test_helpers

        mapping = pipeline.parse(file_path)
        test_content = pipeline.generate_test(mapping, sql_content, effective_schema_dir)

        # Determine test output directory
        if test_output_dir:
            t_dir = test_output_dir
        elif output:
            t_dir = os.path.join(os.path.dirname(output) or ".", "tests")
        else:
            t_dir = "output/tests"

        os.makedirs(t_dir, exist_ok=True)

        # Copy helper files (conftest.py, fake_data_factory.py, sql_test_helper.py)
        copy_test_helpers(t_dir)

        test_path = os.path.join(t_dir, f"test_{mapping.name}_spark.py")
        with open(test_path, "w", encoding="utf-8") as f:
            f.write(test_content)
        click.echo(f"Test generated: {test_path}")
    except Exception as e:
        click.echo(f"Test generation warning: {e}", err=True)


@main.command()
@click.argument("directory")
@click.option("--output-dir", "-o", default="output", help="Output directory")
@click.option("--schema-dir", default=None, type=click.Path(exists=True),
              help="Path to schema directory for schema validation")
@click.option("--test-output-dir", default=None,
              help="Directory for Spark test files (default: {output_dir}/tests/)")
@click.option("--rules-csv", default=None, type=click.Path(exists=True),
              help="Path to transformation rules CSV file")
@click.option("--force", is_flag=True, default=False,
              help="Regenerate all models from scratch (default: only new/changed)")
def batch(directory: str, output_dir: str, schema_dir: str | None,
          test_output_dir: str | None, rules_csv: str | None, force: bool):
    """Generate dbt models + Spark tests + review report from CSV files."""
    from dbt_job_generator.catalog.rule_catalog import TransformationRuleCatalog
    from dbt_job_generator.generation_log import (
        GenerationLog, compute_file_hash, load_generation_log, save_generation_log,
    )

    effective_schema_dir = schema_dir or "schemas"

    # Load transformation rules from CSV if provided
    catalog = None
    if rules_csv:
        catalog = TransformationRuleCatalog.load_from_csv(rules_csv)

    pipeline = Pipeline(schema_dir=effective_schema_dir, catalog=catalog)

    # Load generation log (for incremental mode)
    gen_log = GenerationLog() if force else load_generation_log(output_dir)

    result = pipeline.generate_batch(directory)

    # Determine which models to write based on generation log
    os.makedirs(output_dir, exist_ok=True)
    new_count = 0
    changed_count = 0
    skipped_count = 0
    written_models: list[str] = []

    # Build mapping file path lookup
    mapping_files = {}
    try:
        for f in sorted(os.listdir(directory)):
            if f.lower().endswith(".csv"):
                mapping_files[f] = os.path.join(directory, f)
    except OSError:
        pass

    for model in result.successful:
        model_name = model.model_name
        out_path = os.path.join(output_dir, f"{model_name}.sql")

        # Find the mapping file for this model to compute hash
        mapping_path = None
        for fname, fpath in mapping_files.items():
            if model_name in fname.lower().replace(" ", "_"):
                mapping_path = fpath
                break

        if not force and mapping_path:
            current_hash = compute_file_hash(mapping_path)
            status = gen_log.should_generate(model_name, current_hash)
            if status == "unchanged":
                skipped_count += 1
                continue
            elif status == "new":
                new_count += 1
            else:
                changed_count += 1
        else:
            current_hash = compute_file_hash(mapping_path) if mapping_path else ""
            new_count += 1

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(model.sql_content)
        written_models.append(model_name)

        # Update generation log
        gen_log.update(
            model_name, current_hash,
            sql_file=f"{model_name}.sql",
            test_file=f"test_{model_name}_spark.py",
        )

    # Write test files for written models only
    t_dir = test_output_dir or os.path.join(output_dir, "tests")
    if result.test_files:
        os.makedirs(t_dir, exist_ok=True)

        # Copy helper files (conftest.py, fake_data_factory.py, sql_test_helper.py)
        from dbt_job_generator.spark_test_gen import copy_test_helpers
        copy_test_helpers(t_dir)

        for model_name in written_models:
            if model_name in result.test_files:
                test_path = os.path.join(t_dir, f"test_{model_name}_spark.py")
                with open(test_path, "w", encoding="utf-8") as f:
                    f.write(result.test_files[model_name])

    # Save generation log
    save_generation_log(output_dir, gen_log)

    # Generate and write review report
    report = pipeline.generate_report(result)
    report_path = _write_review_report(report, output_dir)

    # Print summary
    s = result.summary
    click.echo(
        f"Total: {s.total}, Success: {s.success_count}, "
        f"Errors: {s.error_count}, Warnings: {s.warning_count}"
    )

    for err in s.errors:
        click.echo(f"  BLOCK: {err}", err=True)

    for warn in s.warnings:
        click.echo(f"  WARNING: {warn}")

    if result.successful:
        click.echo(f"Output written to: {output_dir}/")
        if skipped_count > 0:
            click.echo(f"  ({new_count} new, {changed_count} changed, {skipped_count} unchanged skipped)")
        else:
            click.echo(f"  ({new_count + changed_count} files written)")
        click.echo(f"Test files written to: {t_dir}/")
        click.echo(f"Review report: {report_path}")


@main.command()
@click.argument("file_path")
@click.option("--schema-dir", default=None, type=click.Path(exists=True),
              help="Path to schema directory for schema validation")
def validate(file_path: str, schema_dir: str | None):
    """Validate a mapping CSV file without generating."""
    pipeline = Pipeline(schema_dir=schema_dir)

    try:
        result = pipeline.validate(file_path, schema_dir=schema_dir)
    except ParseError as e:
        click.echo(f"Parse error: {e}", err=True)
        sys.exit(1)

    has_blocks = len(result.block_errors) > 0

    if not has_blocks and not result.has_warnings:
        click.echo("Validation passed.")
    else:
        if has_blocks:
            click.echo("Validation failed (BLOCK errors found):")
            for be in result.block_errors:
                click.echo(f"  BLOCK [{be.error_type}]: {be.message}")

        if result.has_warnings:
            if not has_blocks:
                click.echo("Validation passed with warnings:")
            for w in result.warnings:
                wtype = f" [{w.warning_type}]" if w.warning_type else ""
                click.echo(f"  WARNING{wtype}: {w.message}")

        if has_blocks:
            sys.exit(1)


@main.command("validate-batch")
@click.argument("directory")
@click.option("--schema-dir", default=None, type=click.Path(exists=True),
              help="Path to schema directory for schema validation")
def validate_batch(directory: str, schema_dir: str | None):
    """Validate all mapping CSV files in a directory without generating."""
    validator = MappingValidator()
    context = ProjectContext()

    result = validator.validate_batch(directory, context, schema_dir=schema_dir)

    click.echo(
        f"Total: {result.total}, Valid: {result.valid_count}, "
        f"Warnings: {result.warning_count}, Blocked: {result.blocked_count}"
    )

    for filename, vr in result.results.items():
        if vr.block_errors:
            click.echo(f"\n  {filename}: BLOCKED")
            for be in vr.block_errors:
                click.echo(f"    BLOCK [{be.error_type}]: {be.message}")
        elif vr.has_warnings:
            click.echo(f"\n  {filename}: WARNING")
            for w in vr.warnings:
                wtype = f" [{w.warning_type}]" if w.warning_type else ""
                click.echo(f"    WARNING{wtype}: {w.message}")

    if result.blocked_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
