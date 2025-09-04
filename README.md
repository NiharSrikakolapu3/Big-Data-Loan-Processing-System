# Big Data Loan Processing System

This project implements a **gRPC-based loan processing system** that interacts with **HDFS** and **MySQL**, processes loan data for Wisconsin counties, and provides RPC methods to query and manipulate the data. All services run in **Docker containers** for easy setup and testing.

## Repository

**Clone the repository:**

```bash
git clone git@github.com:NiharSrikakolapu3/Big-Data-Loan-Processing-System.git
cd Big-Data-Loan-Processing-System
Prerequisites
Docker v20+

Docker Compose v2+

Python 3.12 (optional if running client inside container)


**Setup:**
1. Set Project Environment Variable

Copy code
export PROJECT=p4
This is required by the Docker Compose file to name all images and services.

2. Create Virtual Environment and Install Python Requirements
If you want to run the client or other Python scripts outside the container:


Copy code
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
3. Build Docker Images

Copy code
sudo docker-compose build
This builds the following images:

p4-hdfs

p4-nn (NameNode)

p4-dn (DataNodes)

p4-mysql

p4-server (gRPC Server)

4. Start Docker Compose Cluster
Copy code
sudo docker-compose up -d
This starts:

3 DataNodes: p4-dn-1, p4-dn-2, p4-dn-3

1 NameNode: p4-nn-1

MySQL: p4-mysql-1

gRPC Server: p4-server-1

Check server logs:


Copy code
sudo docker logs -f p4-server-1
Running Client Commands
Enter the server container:


Copy code
sudo docker exec -it p4-server-1 bash
Part 1: Upload to HDFS (DbToHdfs)

Copy code
python3 client.py DbToHdfs
Joins loans and loan_types tables in MySQL.

Filters loans with loan_amount between 30,000 and 800,000.

Writes /hdma-wi-2021.parquet to HDFS with 2x replication and 1 MB block size.

Verify upload:
Copy code
hdfs dfs -du -h /hdma-wi-2021.parquet
Expected file size: ~28–30 MB

Part 2: Check Block Locations (BlockLocations)
Copy code
python3 client.py BlockLocations -f /hdma-wi-2021.parquet
Returns a dictionary showing block distribution across DataNodes, e.g.:

json
Copy code
{"7eb74ce67e75": 15, "f7747b42d254": 7, "39750756065d": 8}
Part 3: Calculate Average Loan for a County (CalcAvgLoan)
Copy code
python3 client.py CalcAvgLoan -c <county_code>
Example:

bash
Copy code
python3 client.py CalcAvgLoan -c 55001
Filters /hdma-wi-2021.parquet by county_code.

Computes average loan amount.

Writes partitions/<county_code>.parquet for reuse.

Returns avg_loan and source (create, reuse, recreate).

Part 4: Fault Tolerance Test
Kill a DataNode:


Copy code
sudo docker kill p4-dn-1
Re-run CalcAvgLoan for a county with 1x replication:


Copy code
python3 client.py CalcAvgLoan -c 55001
If the county-specific file was lost, source will show recreate
