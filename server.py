import grpc
import lender_pb2
import lender_pb2_grpc
import mysql.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
import logging
import requests
from concurrent import futures
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LenderServicer(lender_pb2_grpc.LenderServicer):
    
    def DbToHdfs(self, request, context):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"DbToHdfs: Attempt {attempt + 1}")
                engine = create_engine("mysql+mysqlconnector://root:abc@mysql:3306/CS544")
                conn = engine.connect()
                
                query = text("""
                    SELECT *
                    FROM loans
                    INNER JOIN loan_types ON loans.loan_type_id = loan_types.id
                    WHERE loans.loan_amount > 30000 AND loans.loan_amount < 800000
                """)
                
                df = pd.read_sql(query, conn)
                conn.close()
                
                logger.info(f"DbToHdfs: Read {len(df)} rows from database")
                
                table = pa.Table.from_pandas(df)
                hdfs = pa.fs.HadoopFileSystem(
                    host="boss",
                    user="root",
                    port=9000,
                    replication=2,
                    default_block_size=1024*1024
                )
                
                with hdfs.open_output_stream("/hdma-wi-2021.parquet") as f:
                    pq.write_table(table, f)
                
                logger.info("DbToHdfs: Successfully wrote to HDFS")
                return lender_pb2.StatusString(
                    status=f"DbToHdfs: Successfully wrote {len(df)} rows"
                )

            except Exception as e:
                logger.error(f"DbToHdfs: Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)
                else:
                    return lender_pb2.StatusString(
                        status=f"ERROR: Could not complete operation after {max_retries} attempts: {str(e)}"
                    )
    
    def BlockLocations(self, request, context):
        filepath = request.path
        webhdfs_url = f"http://boss:9870/webhdfs/v1{filepath}?op=GETFILEBLOCKLOCATIONS"
        
        try:
            logger.info(f"BlockLocations: Fetching block locations for {filepath}")
            response = requests.get(webhdfs_url)
            response.raise_for_status()
            result = response.json()
            
            blocks = {} 
            block_locations = result.get("BlockLocations", {}).get("BlockLocation", [])
            
            for block in block_locations:
                hosts = block.get("hosts", [])
                for host in hosts:
                    blocks[host] = blocks.get(host, 0) + 1
            
            logger.info(f"BlockLocations: Found {len(blocks)} datanodes with blocks")
            return lender_pb2.BlockLocationsResp(block_entries=blocks, error="")
            
        except Exception as e:
            logger.error(f"BlockLocations: Error - {e}")
            return lender_pb2.BlockLocationsResp(block_entries={}, error=str(e))

    def CalcAvgLoan(self, request, context):
        county_code = request.county_code
        partition_path = f"/partitions/{county_code}.parquet"
        main_file_path = "/hdma-wi-2021.parquet"
        
        try:
            hdfs = pa.fs.HadoopFileSystem(host="boss", user="root", port=9000)
            
            # Try to read from partition file first (reuse scenario)
            try:
                logger.info(f"CalcAvgLoan: Attempting to read partition file for county {county_code}")
                with hdfs.open_input_file(partition_path) as f:
                    table = pq.read_table(f)
                
                avg_loan = int(table.column("loan_amount").to_numpy().mean())
                logger.info(f"CalcAvgLoan: Reused partition file for county {county_code}, avg={avg_loan}")
                return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source="reuse", error="")
                
            except FileNotFoundError:
                # Partition doesn't exist, create it from main file
                logger.info(f"CalcAvgLoan: Partition not found for county {county_code}, creating from main file")
                source = "create"
                
            except OSError as oe:
                # Partition exists but corrupted/unavailable (DataNode failure)
                logger.warning(f"CalcAvgLoan: Partition corrupted for county {county_code}, recreating: {oe}")
                source = "recreate"
            
            # Read from main file and create/recreate partition
            logger.info(f"CalcAvgLoan: Reading main file for county {county_code}")
            with hdfs.open_input_file(main_file_path) as f:
                table = pq.read_table(f, filters=[("county_code", "=", county_code)])
            
            avg_loan = int(table.column("loan_amount").to_numpy().mean())
            
            # Write partition file with 1x replication
            hdfs_write = pa.fs.HadoopFileSystem(
                host="boss",
                user="root",
                port=9000,
                replication=1,
                default_block_size=1024*1024
            )
            
            with hdfs_write.open_output_stream(partition_path) as f:
                pq.write_table(table, f)
            
            logger.info(f"CalcAvgLoan: Created partition for county {county_code}, avg={avg_loan}, source={source}")
            return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source, error="")
            
        except Exception as e:
            logger.error(f"CalcAvgLoan: Unexpected error for county {county_code}: {e}")
            return lender_pb2.CalcAvgLoanResp(avg_loan=0, source="", error=str(e))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port('[::]:5000')
    logger.info("gRPC server running on port 5000")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
