import grpc
from concurrent import futures
import lender_pb2
import lender_pb2_grpc
import pandas as pd
from sqlalchemy import create_engine, text
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
import time
import requests  # This is to interact with HDFS
import math


class LenderServicer(lender_pb2_grpc.LenderServicer):
    def __init__(self):
        self.alreadyVisited = {}

    def DbToHdfs(self, request, context):
        
        engine = create_engine("mysql+mysqlconnector://root:abc@mysql:3306/CS544")
        retries = 5  
        sleep_time = 5  # Time to wait before retrying

        for x in range(retries):
            try:
                conn = engine.connect()

                # Query to perform inner join and filter results
                join_query = text("""
                SELECT loans.*, loan_types.loan_type_name 
                FROM loans
                INNER JOIN loan_types 
                ON loans.loan_type_id = loan_types.id
                WHERE loans.loan_amount > 30000 AND loans.loan_amount < 800000;
                """)

                theResult = conn.execute(join_query)
                df = pd.DataFrame(theResult.fetchall(), columns=theResult.keys())
                conn.close()

                # Check if the number of rows is correct
                if len(df) != 426716:
                    raise Exception(f"Expected 426716 rows, but got {len(df)}")

                # HDFS conversion and file writing
                table = pa.Table.from_pandas(df)
                hdfs = pa.fs.HadoopFileSystem(host="nn", port=9000, replication=2, default_block_size=1048576)

                # Define the HDFS path
                hdfs_pathfile = "hdfs://nn:9000/hdma-wi-2021.parquet"

                # Write the DataFrame to HDFS as a Parquet file
                with hdfs.open_output_stream(hdfs_pathfile) as f:
                    pq.write_table(table, f)

                
                return lender_pb2.StatusString(status="Upload to HDFS successful")

            except Exception as e:
                if x < retries:
                    print(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)

    def BlockLocations(self, request, context):
        thePath = request.path

        try:
            r = requests.get(f"http://nn:9870/webhdfs/v1/{thePath}?op=GETFILEBLOCKLOCATIONS")
            r.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the JSON response
            keyValues = r.json()

            # Initialize an empty dictionary to hold the count of blocks per DataNode 
            block_count = {}

            for block in keyValues['BlockLocations']['BlockLocation']:
                for host in block['hosts']:
                    theKey = host.split(":")[0]  # Split at the :

                    # Update the key/block value 
                    if theKey in block_count:
                        block_count[theKey] += 1
                    else:
                        block_count[theKey] = 1

            return lender_pb2.BlockLocationsResp(
                block_entries=block_count,
                error=""
            )

        except Exception as e:
            return lender_pb2.BlockLocationsResp(
                block_entries={}, 
                error=f"Error retrieving block locations: {str(e)}"  # Return the error message
            )

    def CalcAvgLoan(self, request, context):
        county_code = request.county_code
        county_file_path = f"hdfs://nn:9000/partitions/{county_code}.parquet"
        hdfs2 = pa.fs.HadoopFileSystem("nn", 9000, replication=1, default_block_size=1048576)
        toFilter = [('county_code', '=', county_code)]
        # Specifying the file to read from
        hdfs_pathfile = "hdfs://nn:9000/hdma-wi-2021.parquet"
                # Read from a table
        hdfs = pa.fs.HadoopFileSystem("nn", 9000)
        try:
            if county_code not in self.alreadyVisited:
                with hdfs.open_input_file(hdfs_pathfile) as f:
                    # Read the filtered table
                    table = pq.read_table(f, filters=toFilter)
                    loanAmounts = table['loan_amount'].to_pandas().dropna()
                    if loanAmounts.empty:
                        avg_loan=0
                    else:
                        avg_loan=int(loanAmounts.mean())

                    source = "create"
                    # Write the filtered table to a county-specific Parquet file
                    with hdfs2.open_output_stream(county_file_path) as t:
                        pq.write_table(table, t)

                    # Used for storing
                    self.alreadyVisited[county_code] = avg_loan 

                    return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source, error="")

            else:
                # Read from the county-specific file
                with hdfs2.open_input_file(county_file_path) as f:
                    table = pq.read_table(f)
                    loanAmounts = table['loan_amount'].to_pandas().dropna()
                    if loanAmounts.empty:
                        avg_loan=0
                    else:
                        avg_loan=int(loanAmounts.mean())

                    source = "reuse"
                    return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source, error="")
        except OSError as e:
         print(f"An error was caused by killing the process id:{str(e)}")
         #time.sleep(5)
         with hdfs.open_input_file(hdfs_pathfile) as f:
                    # Read the filtered table
                    table = pq.read_table(f, filters=toFilter)
                    loanAmounts = table['loan_amount'].to_pandas().dropna()
                    if loanAmounts.empty:
                        avg_loan=0
                    else:
                        avg_loan=int(loanAmounts.mean())

                    source = "recreate"
                    # Write the filtered table to a county-specific Parquet file
                    with hdfs2.open_output_stream(county_file_path) as t:
                        pq.write_table(table, t)

                    # Used for storing
                    self.alreadyVisited[county_code] = avg_loan 

                    return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source, error="")


        except Exception as e:
            return lender_pb2.CalcAvgLoanResp(avg_loan=0, source="", error="Unable to do CalcAvgLoan correctly")


# Start the gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
server.add_insecure_port("0.0.0.0:5000")
print("Server started on port 5000")
server.start()
server.wait_for_termination()