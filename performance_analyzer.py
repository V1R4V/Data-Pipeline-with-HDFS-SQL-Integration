#!/usr/bin/env python3
import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt
import os
COUNTY_CODES = [55001, 55003, 55027, 55059, 55133]
SERVER_CONTAINER = "p4-server-1"
OUTPUT_DIR = "/app/outputs"

def run_command(command):
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {command}")
        print(f"Error: {e.stderr}")
        raise

def delete_partitions():
    print("Deleting /partitions directory for a fair test...")
    try:
        # Try to delete, but don't fail if it doesn't exist
        cmd = f"docker exec {SERVER_CONTAINER} hdfs dfs -rm -r /partitions"
        subprocess.run(cmd, shell=True, capture_output=True)
        print("Partitions directory deleted successfully")
    except Exception as e:
        print(f"Note: Could not delete partitions (may not exist yet): {e}")

def time_calc_avg_loan(county_code):
    cmd = f"docker exec {SERVER_CONTAINER} python3 /client.py CalcAvgLoan -c {county_code}"
    
    start_time = time.monotonic()
    run_command(cmd)
    end_time = time.monotonic()
    
    elapsed = end_time - start_time
    return elapsed

def measure_performance():
    create_times = []
    reuse_times = []
    
    print("\n" + "="*60)
    print("PERFORMANCE MEASUREMENT")
    print("="*60)
    
    for county_code in COUNTY_CODES:
        print(f"\nTesting county {county_code}...")
        print(f"  First call (CREATE)...", end=" ", flush=True)
        create_time = time_calc_avg_loan(county_code)
        create_times.append(create_time)
        print(f"{create_time:.3f}s")
        print(f"  Second call (REUSE)...", end=" ", flush=True)
        reuse_time = time_calc_avg_loan(county_code)
        reuse_times.append(reuse_time)
        print(f"{reuse_time:.3f}s")
        
        print(f"  Speedup: {create_time/reuse_time:.2f}x")
    
    return create_times, reuse_times

def calculate_averages(create_times, reuse_times):
    """Calculate average times for create and reuse operations."""
    avg_create = sum(create_times) / len(create_times)
    avg_reuse = sum(reuse_times) / len(reuse_times)
    
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print(f"Average CREATE time: {avg_create:.3f}s")
    print(f"Average REUSE time:  {avg_reuse:.3f}s")
    print(f"Average Speedup:     {avg_create/avg_reuse:.2f}x")
    print("="*60 + "\n")
    
    return avg_create, avg_reuse

def save_results_csv(avg_create, avg_reuse):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.DataFrame({
        'operation': ['create', 'reuse'],
        'time': [avg_create, avg_reuse]
    })
    csv_path = os.path.join(OUTPUT_DIR, 'performance_results.csv')
    df.to_csv(csv_path, index=False, float_format='%.3f')
    print(f"Results saved to {csv_path}")
    
    return df

def generate_plot(df):
    plt.figure(figsize=(10, 6))
    
    # Create bar chart
    colors = ['#e74c3c', '#2ecc71']  # Red for create, green for reuse
    bars = plt.bar(df['operation'], df['time'], color=colors, alpha=0.8, edgecolor='black')
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.3f}s',
                ha='center', va='bottom', fontsize=12, fontweight='bold')
    plt.xlabel('Operation Type', fontsize=14, fontweight='bold')
    plt.ylabel('Average Time (seconds)', fontsize=14, fontweight='bold')
    plt.title('CalcAvgLoan Performance: Create vs Reuse\nPartitioning Strategy Comparison', 
              fontsize=16, fontweight='bold', pad=20)
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    speedup = df.loc[df['operation'] == 'create', 'time'].values[0] / \
              df.loc[df['operation'] == 'reuse', 'time'].values[0]
    plt.text(0.5, plt.ylim()[1] * 0.9, 
             f'Speedup: {speedup:.2f}x faster with partitioning',
             ha='center', fontsize=12, 
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    plt.tight_layout()
    plot_path = os.path.join(OUTPUT_DIR, 'performance_analysis.png')
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to {plot_path}")
    
    plt.close()

def main():
    """Main execution function."""
    print("="*60)
    print("CalcAvgLoan Performance Analyzer")
    print("="*60)
    delete_partitions()
    create_times, reuse_times = measure_performance()
    avg_create, avg_reuse = calculate_averages(create_times, reuse_times)
    df = save_results_csv(avg_create, avg_reuse)
    generate_plot(df)
    
    print("\n✓ Performance analysis complete!")
    print(f"✓ Check {OUTPUT_DIR} for results\n")

if __name__ == '__main__':
    main()
