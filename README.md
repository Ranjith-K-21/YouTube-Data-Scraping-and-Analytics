# YouTube Data Scraper and Analyzer
This project is designed to scrape data from YouTube channels, including video details and comments, and perform analytics on the fetched data. It utilizes the YouTube Data API v3 for fetching data and provides insights through Streamlit-based user interface.

## Features
- **Data Scraping:** Fetches channel information, video details, and comments using the YouTube Data API.
- **Data Analytics:** Analyzes the fetched data by running various SQL queries to extract insights.
- **Streamlit Interface:** Provides an interactive user interface powered by Streamlit for easy navigation and query execution.
- M**ySQL Database Integration:** Stores the fetched data in a MySQL database for efficient storage and retrieval.

## Prerequisites
Before running the application, make sure you have the following prerequisites installed:

- Python 3.x
- Required Python packages: **googleapiclient**, **pandas**, **streamlit**, **sqlalchemy**, **mysql-connector-python**, **numpy**, **isodate**
  
## Setup
1. Clone the repository to your local machine:
```bash
git clone https://github.com/Ranjith-K-21/YouTube-Data-Scraping-and-Analytics.git
```
2. Install the required Python packages:
```bash
pip install -r requirements.txt
```
3. Obtain an API key from the Google Cloud Console and replace **"Your API Key"** in the code with your actual API key.

4. Set up a MySQL database and replace **"username"** and **"password"** in the code with your MySQL database credentials.

## Usage
1. Run the application:
```bash
streamlit run YouTube_Data_Insights.py
```

2. Use the sidebar navigation to switch between the **"Data Scraping"** and **"Data Analytics"** sections.

3. Enter the required inputs and click on buttons to start scraping data or running queries.

4. View the results displayed on the Streamlit interface.

## Contributing
Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.
