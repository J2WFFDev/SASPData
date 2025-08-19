import sys
from src import ingest as ingest_module

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m src.cli [ingest|silver|gold|all]")
        sys.exit(1)
    cmd = sys.argv[1].lower()
    if cmd == "ingest":
        ingest_module.run_ingest()
    else:
        print(f"Command {cmd} not implemented yet.")

if __name__ == "__main__":
    main()
