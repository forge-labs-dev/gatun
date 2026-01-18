import os

from gatun.client import GatunClient


def main():
    print("--- Crash Test: Connecting ---")
    client = GatunClient()
    client.connect()

    # Create 5 objects
    for i in range(5):
        obj = client.create_object("java.util.ArrayList")
        print(f"Created Orphan Candidate: {obj.object_id}")

    print("--- SIMULATING CRASH (kill -9) ---")
    print("Check your Java logs. You should see 'Cleaning up 5 orphaned objects'")

    # Hard kill the process immediately (bypasses weakref and finally blocks)
    os._exit(1)


if __name__ == "__main__":
    main()
