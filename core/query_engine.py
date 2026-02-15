import time

class QueryEngine:
    def __init__(self, analyzer, ingestion_queue):
        self.analyzer = analyzer
        self.queue = ingestion_queue
        self.start_time = time.time()

    def process_command(self, command_str):
        args = command_str.strip().split()
        if not args:
            return ""

        cmd = args[0].lower()

        if cmd == "help":
            return (
                "\n" + "="*60 + "\n"
                "  AVAILABLE COMMANDS\n"
                + "="*60 + "\n\n"
                "  status\n"
                "    Shows system uptime, total records processed, and active field count.\n\n"
                "  stats <field>\n"
                "    Displays detailed analytics for a specific field including:\n"
                "    - Frequency ratio (how often it appears)\n"
                "    - Type stability (stable or mixed types)\n"
                "    - Detected type (str, int, float, etc.)\n"
                "    - Uniqueness ratio\n"
                "    Example: stats age\n\n"
                "  queue\n"
                "    Shows number of records currently waiting in ingestion buffer.\n\n"
                "  all_stats\n"
                "    Displays summary statistics for all tracked fields.\n\n"
                "  exit\n"
                "    Gracefully shuts down all worker threads and closes connections.\n"
                + "="*60 + "\n"
            )

        elif cmd == "status":
            uptime = int(time.time() - self.start_time)
            msg = (
                f"System Uptime: {uptime} seconds\n"
                f"Total Records Processed: {self.analyzer.total_records_processed}\n"
                f"Active Fields Tracked: {len(self.analyzer.field_stats)}"
            )
            return msg

        elif cmd == "queue":
            return f"Current Queue Size: {self.queue.qsize()} records pending processing."

        elif cmd == "stats":
            if len(args) < 2:
                return "Usage: stats <field_name>\nExample: stats age"
            field = args[1]
            stats = self.analyzer.get_schema_stats()
            if field in stats:
                s = stats[field]
                return (
                    f"\n{'='*60}\n"
                    f"  FIELD ANALYSIS: '{field}'\n"
                    f"{'='*60}\n"
                    f"  Frequency Ratio:  {s['frequency_ratio']:.2%} (appears in {s['frequency_ratio']*100:.1f}% of records)\n"
                    f"  Type Stability:   {s['type_stability']}\n"
                    f"  Detected Type:    {s['detected_type']}\n"
                    f"  Is Nested:        {s['is_nested']}\n"
                    f"  Unique Ratio:     {s['unique_ratio']:.2%}\n"
                    f"  Total Count:      {s['count']} occurrences\n"
                    f"{'='*60}\n"
                )
            else:
                return f"Field '{field}' not found. Type 'all_stats' to see available fields."

        elif cmd == "all_stats":
            stats = self.analyzer.get_schema_stats()
            if not stats:
                return "No field statistics available yet. Wait for data to be ingested."
            
            result = f"\n{'='*80}\n  ALL FIELD STATISTICS ({len(stats)} fields tracked)\n{'='*80}\n"
            for field_name in sorted(stats.keys()):
                s = stats[field_name]
                result += (
                    f"\n[{field_name}]\n"
                    f"  Frequency: {s['frequency_ratio']:.2%}  |  Type: {s['detected_type']}  |  "
                    f"Stability: {s['type_stability']}  |  Unique: {s['unique_ratio']:.2%}  |  "
                    f"Count: {s['count']}\n"
                )
            result += f"{'='*80}\n"
            return result

        else:
            return f"Unknown command: '{cmd}'. Type 'help' for options."
