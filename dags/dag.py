from airflow import DAG
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Referer": "https://www.google.com/",
}


def get_amazon_data_books(num_books, ti):
    # This site is designed for scraping practice!
    base_url = "http://books.toscrape.com/catalogue/page-{}.html"
    books = []
    page = 1

    while len(books) < num_books and page <= 5:
        url = base_url.format(page)
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            # The site uses <article class="product_pod"> for each book
            book_containers = soup.find_all("article", class_="product_pod")

            for book in book_containers:
                # Find title in the <h3><a> tag
                title = book.h3.a["title"]
                # Find price in <p class="price_color">
                price = book.find("p", class_="price_color").text.strip()
                # Find rating in <p class="star-rating ...">
                rating_class = book.find("p", class_="star-rating")["class"]
                rating = rating_class[1]  # e.g., "Three", "Four"

                # We'll just put "N/A" for author since this site doesn't list them on the main page
                books.append(
                    {
                        "Title": title,
                        "Author": "Various",
                        "Price": price,
                        "Rating": rating,
                    }
                )
            page += 1
        else:
            print(f"Failed to reach page {page}")
            break

    df = pd.DataFrame(books[:num_books])
    print(f"Successfully scraped {len(df)} books.")
    ti.xcom_push(key="book_data", value=df.to_dict("records"))


# 3) create and store data in table on postgres (load)


def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key="book_data", task_ids="fetch_book_data")
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id="books_connection")
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(
            insert_query,
            parameters=(book["Title"], book["Author"], book["Price"], book["Rating"]),
        )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fetch_and_store_amazon_books",
    default_args=default_args,
    description="A simple DAG to fetch and store Amazon book data in postgres",
    schedule="@daily",
)


# operators : Python Operator and PostgresOperator
# hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id="fetch_book_data",
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="books_connection",
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
    task_id="insert_book_data",
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

# dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task
