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
                "Available Commands:\n"
                "  status          - Show system uptime and total processed records.\n"
                "  queue           - Show current number of records waiting in queue.\n"
                "  stats <field>   - Show statistics for a specific field.\n"
                "  all_stats       - Show stats for all fields (summary).\n"
                "  exit            - Stop the system.\n"
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
                return "Usage: stats <field_name>"
            field = args[1]
            stats = self.analyzer.get_schema_stats()
            if field in stats:
                return f"Stats for '{field}': {stats[field]}"
            else:
                return f"Field '{field}' not found. Note: Fields are case-sensitive."

        elif cmd == "all_stats":
             stats = self.analyzer.get_schema_stats()
             return str(stats)

        else:
            return f"Unknown command: '{cmd}'. Type 'help' for options."
