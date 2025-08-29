
#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import time
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#1) fetch amazon data (extract) 2) clean data (transform)

# headers = {
#     "Referer": 'https://www.amazon.com/',
#     "Sec-Ch-Ua": "Not_A Brand",
#     "Sec-Ch-Ua-Mobile": "?0",
#     "Sec-Ch-Ua-Platform": "macOS",
#     'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
# }
# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
#     "Accept-Language": "en-US,en;q=0.9",
#     "Accept-Encoding": "gzip, deflate, br",
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#     "Connection": "keep-alive",
#     "DNT": "1",
#     "Upgrade-Insecure-Requests": "1",
#     "Referer": "https://www.amazon.com/"
# }

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.amazon.com/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}


def get_google_books_data(num_books, ti):
    base_url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": "data engineering",   # search keyword
        "maxResults": 40           # max allowed per request
    }

    books = []
    seen_titles = set()

    start_index = 0
    while len(books) < num_books:
        params["startIndex"] = start_index
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            print(items)
            if not items:
                break

            for item in items:
                print(item)
                volume_info = item.get("volumeInfo", {})
                sale_info = item.get("saleInfo", {})

                title = volume_info.get("title")
                authors = ", ".join(volume_info.get("authors", []))
                rating = volume_info.get("averageRating", None)
                price_info = sale_info.get("listPrice") or sale_info.get("retailPrice")
                price = price_info.get("amount") if price_info else None

                if title and title not in seen_titles:
                    seen_titles.add(title)
                    books.append({
                        "Title": title,
                        "Author": authors,
                        "Price": str(price) if price else "N/A",
                        "Rating": str(rating) if rating else "N/A",
                    })

            start_index += 40  # move to next batch
        else:
            print("Failed to fetch from Google Books API")
            break

    # trim to required count
    books = books[:num_books]

    df = pd.DataFrame(books)
    df.drop_duplicates(subset="Title", inplace=True)

    # Push cleaned data to XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))


def get_amazon_books_data(num_books, ti):
    # Base URL of the Amazon search results for data science books
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"

    books = []
    seen_titles = set()  # To keep track of seen titles

    page = 1
    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        # Send a request to the URL
        response = requests.get(url, headers=headers)
        time.sleep(2)
        print(response.status_code)
        print(response.text[:500]) 

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the content of the request with BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Find book containers (you may need to adjust the class names based on the actual HTML structure)
            #book_containers = soup.find_all("div", {"class": "s-result-item"})
            book_containers = soup.select("div.s-main-slot div.s-result-item")
            print("Found containers:", len(book_containers))
            if book_containers:
                print("First container HTML:", book_containers[0].prettify()[:1000])

            
            # Loop through the book containers and extract data
            for book in book_containers:
                if not book.get("data-asin"):
                    continue

                title_tag = book.select_one("h2.a-size-base-plus.a-spacing-none.a-color-base.a-text-normal > span")
                title = title_tag.text.strip() if title_tag else None

                # Author(s)
                author_div = book.find("div", class_="a-row a-size-base a-color-secondary")
                author = ""
                if author_div:
                    author_links = author_div.find_all("a", class_="a-size-base a-link-normal s-underline-text s-underline-link-text s-link-style")
                    author = ", ".join([a.text.strip() for a in author_links]) if author_links else author_div.text.strip()

                # Price
                price_span = book.find("span", class_="a-price")
                price = None
                if price_span:
                    price_offscreen = price_span.find("span", class_="a-offscreen")
                    price = price_offscreen.text.strip() if price_offscreen else None

                # Rating
                rating_span = book.find("span", class_="a-icon-alt")
                rating = rating_span.text.strip() if rating_span else None

                
                if title and author and price and rating:

                    # Check if title has been seen before
                    if title not in seen_titles:
                        seen_titles.add(title)
                        books.append({
                            "Title": title,
                            "Author": author,
                            "Price": price,
                            "Rating": rating,
                        })
            
            # Increment the page number for the next iteration
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    # Limit to the requested number of books
    books = books[:num_books]
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(books)
    
    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)
    
    # Push the DataFrame to XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))

#3) create and store data in table on postgres (load)
    
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_books_data,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task
