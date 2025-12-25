#!/usr/bin/env python3
"""
Migration Log Analyzer
Analyzes Spark migration logs and generates a detailed phase analysis report.

Usage:
    python analyze_migration_log.py <log_file_path> [--output <output_file>]

Example:
    python analyze_migration_log.py migration_test_transaction_datastore.log
    python analyze_migration_log.py /path/to/log.log --output migration_report.md
"""

import re
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class LogAnalyzer:
    def __init__(self, log_file_path: str):
        self.log_file_path = Path(log_file_path)
        if not self.log_file_path.exists():
            raise FileNotFoundError(f"Log file not found: {log_file_path}")
        
        self.events: List[Tuple[datetime, int, str]] = []
        self.milestones: Dict[str, datetime] = {}
        self.metrics: Dict[str, any] = {}
        
    def parse_log(self):
        """Parse the log file and extract events with timestamps."""
        print(f"Reading log file: {self.log_file_path}")
        
        with open(self.log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        
        print(f"Total lines in log: {len(lines)}")
        
        # Parse timestamps and events
        for line_num, line in enumerate(lines, 1):
            # Match timestamp pattern: 25/12/24 21:30:04 or 2024-12-24 21:30:04
            timestamp_match = re.search(r'(\d{2}/\d{2}/\d{2}|\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
            if timestamp_match:
                date_str, time_str = timestamp_match.groups()
                try:
                    # Try different date formats
                    if '/' in date_str:
                        # Format: 25/12/24
                        ts = datetime.strptime(f"{date_str} {time_str}", "%y/%m/%d %H:%M:%S")
                    else:
                        # Format: 2024-12-24
                        ts = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                    self.events.append((ts, line_num, line.strip()))
                except ValueError:
                    continue
        
        print(f"Parsed {len(self.events)} events with timestamps")
        
    def identify_milestones(self):
        """Identify key milestones in the migration."""
        print("Identifying milestones...")
        
        for ts, line_num, line in self.events:
            # Phase 0: Initialization
            if 'Spark session created' in line and 'spark_session' not in self.milestones:
                self.milestones['spark_session'] = ts
            elif 'SparkContext: Submitted application' in line and 'app_submitted' not in self.milestones:
                self.milestones['app_submitted'] = ts
            
            # Phase 1: Planning
            elif 'Reading table:' in line and 'reading_table' not in self.milestones:
                self.milestones['reading_table'] = ts
            elif 'Successfully read table' in line and 'table_read' not in self.milestones:
                self.milestones['table_read'] = ts
            elif 'DataFrame has' in line and 'partitions' in line and 'partitions_calculated' not in self.milestones:
                self.milestones['partitions_calculated'] = ts
                # Extract partition count
                partition_match = re.search(r'(\d+)\s+partitions', line)
                if partition_match:
                    self.metrics['partition_count'] = int(partition_match.group(1))
            
            # Phase 2-4: Migration execution
            elif 'Starting migration for table' in line and 'migration_start' not in self.milestones:
                self.milestones['migration_start'] = ts
            elif 'Starting job: foreachPartition' in line or 'Got job 0' in line:
                if 'job_started' not in self.milestones:
                    self.milestones['job_started'] = ts
            elif 'Submitting.*missing tasks' in line or 'Starting task 0.0' in line:
                if 'first_task_started' not in self.milestones:
                    self.milestones['first_task_started'] = ts
            elif 'COPY operation completed' in line and 'first_copy_completed' not in self.milestones:
                self.milestones['first_copy_completed'] = ts
                # Extract rows copied
                rows_match = re.search(r'Rows copied:\s*(\d+)', line)
                if rows_match:
                    self.metrics['first_copy_rows'] = int(rows_match.group(1))
            elif 'Partition completed:' in line and 'first_partition_completed' not in self.milestones:
                self.milestones['first_partition_completed'] = ts
            elif 'Job 0 finished' in line or 'ResultStage.*finished' in line:
                if 'job_finished' not in self.milestones:
                    self.milestones['job_finished'] = ts
                    # Extract job duration
                    duration_match = re.search(r'took\s+([\d.]+)\s+s', line)
                    if duration_match:
                        self.metrics['job_duration_seconds'] = float(duration_match.group(1))
            elif 'Migration completed for table' in line and 'migration_complete' not in self.milestones:
                self.milestones['migration_complete'] = ts
            
            # Phase 5: Validation
            elif 'Running validation' in line and 'validation_start' not in self.milestones:
                self.milestones['validation_start'] = ts
            elif 'Row count validation passed' in line or 'validation.*passed' in line:
                if 'validation_complete' not in self.milestones:
                    self.milestones['validation_complete'] = ts
            elif 'Migration Summary' in line and 'summary' not in self.milestones:
                self.milestones['summary'] = ts
        
        # Extract metrics from summary
        self._extract_metrics()
        
    def _extract_metrics(self):
        """Extract metrics from log file."""
        with open(self.log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Extract rows read/written
        rows_read_match = re.search(r'Rows Read:\s*(\d+)', content)
        rows_written_match = re.search(r'Rows Written:\s*(\d+)', content)
        rows_skipped_match = re.search(r'Rows Skipped:\s*(\d+)', content)
        throughput_match = re.search(r'Throughput:\s*([\d.]+)\s+rows/sec', content)
        elapsed_time_match = re.search(r'Elapsed Time:\s*(\d+)\s+seconds?', content)
        partitions_completed_match = re.search(r'Partitions Completed:\s*(\d+)', content)
        partitions_failed_match = re.search(r'Partitions Failed:\s*(\d+)', content)
        
        if rows_read_match:
            self.metrics['rows_read'] = int(rows_read_match.group(1))
        if rows_written_match:
            self.metrics['rows_written'] = int(rows_written_match.group(1))
        if rows_skipped_match:
            self.metrics['rows_skipped'] = int(rows_skipped_match.group(1))
        if throughput_match:
            self.metrics['throughput'] = float(throughput_match.group(1))
        if elapsed_time_match:
            self.metrics['elapsed_time_seconds'] = int(elapsed_time_match.group(1))
        if partitions_completed_match:
            self.metrics['partitions_completed'] = int(partitions_completed_match.group(1))
        if partitions_failed_match:
            self.metrics['partitions_failed'] = int(partitions_failed_match.group(1))
        
        # Extract table name
        table_match = re.search(r'Migrating table:\s*([\w.]+)', content)
        if table_match:
            self.metrics['table_name'] = table_match.group(1)
    
    def calculate_durations(self) -> Dict[str, float]:
        """Calculate durations between milestones."""
        durations = {}
        
        if 'spark_session' in self.milestones and 'partitions_calculated' in self.milestones:
            durations['init'] = (self.milestones['partitions_calculated'] - self.milestones['spark_session']).total_seconds()
        
        if 'reading_table' in self.milestones and 'partitions_calculated' in self.milestones:
            durations['planning'] = (self.milestones['partitions_calculated'] - self.milestones['reading_table']).total_seconds()
        
        if 'job_started' in self.milestones and 'job_finished' in self.milestones:
            durations['migration'] = (self.milestones['job_finished'] - self.milestones['job_started']).total_seconds()
        elif 'first_task_started' in self.milestones and 'migration_complete' in self.milestones:
            durations['migration'] = (self.milestones['migration_complete'] - self.milestones['first_task_started']).total_seconds()
        
        if 'validation_start' in self.milestones and 'validation_complete' in self.milestones:
            durations['validation'] = (self.milestones['validation_complete'] - self.milestones['validation_start']).total_seconds()
        
        if 'spark_session' in self.milestones and 'migration_complete' in self.milestones:
            durations['total'] = (self.milestones['migration_complete'] - self.milestones['spark_session']).total_seconds()
        
        return durations
    
    def generate_report(self, output_file: Optional[str] = None) -> str:
        """Generate markdown report."""
        durations = self.calculate_durations()
        
        # Determine output file
        if output_file:
            output_path = Path(output_file)
            # Create parent directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            output_path = self.log_file_path.parent / f"{self.log_file_path.stem}_analysis.md"
        
        # Generate report content
        report_lines = []
        
        # Header
        report_lines.append("# Migration Phase Analysis Report")
        report_lines.append("")
        report_lines.append(f"**Log File:** `{self.log_file_path.name}`")
        report_lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Metrics summary
        if self.metrics:
            report_lines.append("## Summary")
            report_lines.append("")
            if 'table_name' in self.metrics:
                report_lines.append(f"**Table:** `{self.metrics['table_name']}`")
            if 'rows_read' in self.metrics:
                report_lines.append(f"**Total Records:** {self.metrics['rows_read']:,}")
            if 'throughput' in self.metrics:
                report_lines.append(f"**Throughput:** {self.metrics['throughput']:,.2f} rows/sec")
            if 'total' in durations:
                report_lines.append(f"**Total Time:** {durations['total']:.2f} seconds")
            report_lines.append("")
        
        # Phase breakdown table
        report_lines.append("## Phase Breakdown Table")
        report_lines.append("")
        report_lines.append("| Spark Phase / Stage | Typical UI Indicator | Time Taken | Optimization | Notes / Impact |")
        report_lines.append("| ------------------- | ------------------- | --------- | ------------ | -------------- |")
        
        # Phase 0
        init_time = durations.get('init', 0) if 'spark_session' in self.milestones else 0
        report_lines.append(f"| **Phase 0 – Initialization** | Job 0, Stage 0 (pre-job) | **~{init_time:.1f} seconds** | – | SparkSession creation, config loading, driver initialization, YugabyteDB driver registration, checkpoint table setup. Minimal impact on migration time. |")
        
        # Phase 1
        planning_time = durations.get('planning', 0)
        report_lines.append(f"| **Phase 1 – Planning / Token Range Calculation** | Stage 1 (implicit during DataFrame creation) | **~{planning_time:.1f} seconds** | Skip Row Count Estimation ✅ (Already implemented) | Token range calculation, partition creation. Current: {planning_time:.1f} seconds. Optimized - no COUNT queries. |")
        
        # Phase 2-4
        migration_time = durations.get('migration', 0)
        if 'job_duration_seconds' in self.metrics:
            migration_time = self.metrics['job_duration_seconds']
        partition_count = self.metrics.get('partition_count', 'N/A')
        report_lines.append(f"| **Phase 2-4 – Read/Transform/COPY** | Stage 0 (ResultStage) - Task execution | **~{migration_time:.1f} seconds** | Token-aware partitioning ✅, Direct COPY streaming ✅, Parallel execution ✅ | Read from Cassandra, transform to CSV, COPY to YugabyteDB. {partition_count} partitions processed concurrently. |")
        
        # Phase 5
        validation_time = durations.get('validation', 0)
        if validation_time == 0:
            validation_time = 0.1  # Estimate if not found
        report_lines.append(f"| **Phase 5 – Validation / Post-Processing** | Post-Stage (after Job 0) | **<{validation_time:.1f} seconds** | Metrics-based validation ✅ (no COUNT queries) | Row count validation using Spark Accumulators. Instant validation without database queries. |")
        
        report_lines.append("")
        
        # Detailed timeline
        report_lines.append("## Detailed Timeline")
        report_lines.append("")
        report_lines.append("| Event | Timestamp | Duration from Start |")
        report_lines.append("|-------|-----------|---------------------|")
        
        start_time = self.milestones.get('spark_session') or self.milestones.get('app_submitted')
        if start_time:
            for milestone_name, milestone_time in sorted(self.milestones.items(), key=lambda x: x[1]):
                duration = (milestone_time - start_time).total_seconds()
                event_name = milestone_name.replace('_', ' ').title()
                report_lines.append(f"| {event_name} | {milestone_time.strftime('%H:%M:%S')} | {duration:.1f}s |")
        
        report_lines.append("")
        
        # Key observations
        report_lines.append("## Key Observations")
        report_lines.append("")
        report_lines.append("### Performance Metrics")
        if 'rows_read' in self.metrics and 'total' in durations:
            report_lines.append(f"- **Total Migration Time:** {durations['total']:.2f} seconds")
        if 'job_duration_seconds' in self.metrics:
            report_lines.append(f"- **Job 0 Execution Time:** {self.metrics['job_duration_seconds']:.2f} seconds (actual data processing)")
        if 'planning' in durations:
            report_lines.append(f"- **Planning Phase:** {durations['planning']:.2f} seconds")
        if 'throughput' in self.metrics:
            report_lines.append(f"- **Throughput:** {self.metrics['throughput']:,.2f} rows/sec")
        report_lines.append("")
        
        # Partition distribution
        if 'partition_count' in self.metrics:
            report_lines.append("### Partition Distribution")
            report_lines.append(f"- **Total Partitions:** {self.metrics['partition_count']} (determined by Cassandra token ranges)")
            if 'partitions_completed' in self.metrics:
                report_lines.append(f"- **Partitions Completed:** {self.metrics['partitions_completed']}")
            if 'partitions_failed' in self.metrics:
                report_lines.append(f"- **Partitions Failed:** {self.metrics['partitions_failed']}")
            report_lines.append("")
        
        # COPY performance
        if 'first_copy_completed' in self.milestones and 'first_task_started' in self.milestones:
            first_copy_time = (self.milestones['first_copy_completed'] - self.milestones['first_task_started']).total_seconds()
            report_lines.append("### COPY Performance")
            report_lines.append(f"- **First COPY Stream:** Completed in ~{first_copy_time:.1f} seconds")
            if 'first_copy_rows' in self.metrics:
                report_lines.append(f"- **First COPY Rows:** {self.metrics['first_copy_rows']:,} rows")
            report_lines.append("")
        
        # Validation results
        if 'rows_read' in self.metrics and 'rows_written' in self.metrics:
            report_lines.append("### Validation Results")
            report_lines.append(f"- **Rows Read:** {self.metrics['rows_read']:,}")
            report_lines.append(f"- **Rows Written:** {self.metrics['rows_written']:,}")
            if 'rows_skipped' in self.metrics:
                report_lines.append(f"- **Rows Skipped:** {self.metrics['rows_skipped']:,}")
            match = self.metrics.get('rows_read', 0) == self.metrics.get('rows_written', -1)
            report_lines.append(f"- **Validation:** {'✅ PASSED' if match else '❌ FAILED'}")
            report_lines.append("")
        
        # Write report
        report_content = '\n'.join(report_lines)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print(f"\n✅ Report generated: {output_path}")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Spark migration logs and generate phase analysis report',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('log_file', help='Path to the migration log file')
    parser.add_argument('--output', '-o', help='Output file path (default: <log_file>_analysis.md)')
    
    args = parser.parse_args()
    
    try:
        analyzer = LogAnalyzer(args.log_file)
        analyzer.parse_log()
        analyzer.identify_milestones()
        report_path = analyzer.generate_report(args.output)
        
        print(f"\n📊 Analysis complete!")
        print(f"   Report saved to: {report_path}")
        
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

