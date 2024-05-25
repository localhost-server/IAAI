import os
import concurrent.futures
import boto3
import aiohttp
import asyncio
import multiprocessing
import argparse
import json

s3 = boto3.client(
    's3',
    region_name='ap-southeast-2',
    aws_access_key_id='AKIA2UC3FUYAJIH52IFC',
    aws_secret_access_key='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'
)

def upload_file_to_s3(image_data, image_name):
    """
    Upload a file to an S3 bucket.

    :param image_data: The image data to upload.
    :param image_name: The name of the image file.
    """
    bucket_name = 'cars--iaai'  # Replace with your S3 bucket name
    s3_key = f'{image_name}'  # Use the file name as the object key

    try:
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=image_data)
        # print(f"Uploaded {image_name} to S3 bucket {bucket_name} with key {s3_key}")
        return s3_key  # Return the S3 key for reference
    except Exception as e:
        print(f"Error uploading {image_name} to S3 bucket: {e}")
        return None

# ...

async def download_image(session, url, semaphore):
    async with semaphore, session.get(url) as resp:
        return await resp.read()

async def process_and_upload(link, image_urls):
    if not image_urls:
        print("No images to download.")
        return []
    # Create a semaphore to limit the number of concurrent downloads
    semaphore = asyncio.Semaphore(len(image_urls))

    # Create a session for asynchronous HTTP requests with a persistent connection
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=False)) as session:
        # Download all images in parallel
        downloaded_images = await asyncio.gather(*[download_image(session, url, semaphore) for url in image_urls])

        # Use a ProcessPoolExecutor to upload images to S3 in parallel
        with concurrent.futures.ProcessPoolExecutor(max_workers=len(image_urls)) as executor:
            # Create upload tasks
            upload_tasks = [executor.submit(upload_file_to_s3, image_data, f'{link.replace("/VehicleDetail/","")}{index}.jpg') for index, image_data in enumerate(downloaded_images)]
            
            # Wait for all upload tasks to complete
            uploaded_image_keys = [task.result() for task in concurrent.futures.as_completed(upload_tasks)]

            # Filter out failed uploads (None values)
            uploaded_image_keys = [result for result in uploaded_image_keys if result is not None]

    return uploaded_image_keys


def main():
    parser = argparse.ArgumentParser(description='Download images and upload to S3.')
    parser.add_argument('link', type=str, help='The link to process.')
    parser.add_argument('image_urls', type=str, help='The image URLs to download and upload.')

    args = parser.parse_args()

    link = args.link
    image_urls = json.loads(args.image_urls)

    # Explicitly create and run a new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(process_and_upload(link, image_urls))
    finally:
        loop.close()


if __name__ == "__main__":
    main()