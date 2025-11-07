#!/usr/bin/env python3
"""
Helper script to generate docker-compose.yml with specified number of workers.
Usage: python generate_compose.py 4
"""

import sys

def generate_docker_compose(num_workers):
    """Generate docker-compose.yml with specified number of workers"""
    
    yaml_content = f"""version: '3.8'

services:
  # Coordinator service - orchestrates MapReduce
  coordinator:
    build: .
    container_name: coordinator
    hostname: coordinator
    command: python coordinator.py
    networks:
      - mapreduce-network
    environment:
      - NUM_WORKERS={num_workers}
    volumes:
      - ./txt:/app/txt
    depends_on:
"""
    
    # Add dependencies
    for i in range(1, num_workers + 1):
        yaml_content += f"      - worker-{i}\n"
    
    # Add worker services
    for i in range(1, num_workers + 1):
        port = 18860 + i
        yaml_content += f"""
  worker-{i}:
    build: .
    container_name: worker-{i}
    hostname: worker-{i}
    command: python worker.py
    networks:
      - mapreduce-network
    ports:
      - "{port}:18861"
"""
    
    # Add network configuration
    yaml_content += """
networks:
  mapreduce-network:
    driver: bridge
"""
    
    return yaml_content


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_compose.py <num_workers>")
        print("Example: python generate_compose.py 4")
        sys.exit(1)
    
    try:
        num_workers = int(sys.argv[1])
        if num_workers < 1 or num_workers > 20:
            print("Error: Number of workers must be between 1 and 20")
            sys.exit(1)
    except ValueError:
        print("Error: Please provide a valid number")
        sys.exit(1)
    
    yaml_content = generate_docker_compose(num_workers)
    
    # Write to file
    with open('docker-compose.yml', 'w') as f:
        f.write(yaml_content)
    
    print(f"✓ Generated docker-compose.yml with {num_workers} workers")
    print(f"  Run: docker-compose up --build")


# Quick test
if __name__ == "__main__" and len(sys.argv) == 1:
    print("Testing different configurations:\n")
    for n in [1, 2, 4, 8]:
        print(f"=== {n} Workers ===")
        print(generate_docker_compose(n)[:300] + "...\n")
