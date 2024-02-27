import time
import asyncio
import aiohttp
from pyspark.sql import SparkSession

# Adjusted function with semaphore for rate limiting
async def async_http_call(session, semaphore, row):
    time.sleep(0.001) # Simulate a HTTP call
    return [row.id, row.id * 2] # This double the value generated

async def async_process(list_row, req_per_second):
    semaphore = asyncio.Semaphore(req_per_second)  # Limit the number of concurrent requests
    async with aiohttp.ClientSession() as session:
        tasks = []
        for (index, row) in enumerate(list_row):
            # Pass the semaphore to each task
            task = asyncio.create_task(async_http_call(session, semaphore, row))
            tasks.append(task)
            if len(tasks) % req_per_second == 0:
                await asyncio.sleep(1)  # Sleep to respect the rate limit

        result = await asyncio.gather(*tasks)
        return result

def process_data(list_row):
    result = asyncio.run(async_process(list_row, 1000))
    return result

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    columns = ["id"]
    data = [[item] for item in range(10000)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(["id"])
    df = df.repartition(3) # Number of parallel tasks, each task will generate a pool of 1000 threads => 3000 async requests
    df = df.rdd.mapPartitions(process_data).toDF(["original_id", "new_id"])
    df.show()