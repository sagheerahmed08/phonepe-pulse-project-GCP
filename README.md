# 📊 PhonePe Transaction Insights Project

The **PhonePe Transaction Insights Project** is a data analytics and visualization platform that explores India's digital payment trends using the [PhonePe Pulse dataset](https://github.com/PhonePe/pulse). It extracts, transforms, and visualizes transaction data by **state**, **year**, and **quarter**, offering insights into the growth of India's digital economy.

Built with **Python**, **Google Cloud Storage**, **Pandas**, and **Streamlit**, the project provides an interactive dashboard hosted on Streamlit Cloud.

---

## 🚀 Features

- Extracts data from Google Cloud Storage
- Transforms JSON into structured DataFrames
- Analyzes transactions by state, year, and quarter
- Visualizes trends and transaction amounts
- Interactive dashboard using Streamlit

---

## 🛠️ Tech Stack

- Python 3.11+
- Pandas
- Google Cloud Storage
- Streamlit
- PhonePe Pulse Dataset

---

## 📂 Folder Structure

📦PhonePe-Transaction-Insights
┣ 📁src
┃ ┣ 📜Dashboard.py
┃ ┗ 📜Data_Extraction.py
┣ 📁src
  ┣ 📄Top_transaction.csv
┗ 📄README.md

---

## ⚙️ Setup Instructions

1. **Clone the repository**

```bash
git clone https://github.com/your-username/PhonePe-Transaction-Insights.git
cd PhonePe-Transaction-Insights
```
2. **Create virtual environment & install dependencies**
```python -m venv env
# For Windows
env\Scripts\activate
# For Mac/Linux
source env/bin/activate

pip install -r requirements.txt
```
3.**Set up Google Cloud credentials**
```
export GOOGLE_APPLICATION_CREDENTIALS="path-to-your-service-account-key.json"
```

4. **Run the Streamlit dashboard**
```
streamlit run src/Dashboard.py
```

🌐 Deployment
The dashboard is deployed on Streamlit Cloud
[Dashboard link:](https://phonepe-sagi.streamlit.app)


📈 Sample Insights

- Maharashtra, Karnataka, and Tamil Nadu lead in transaction volume.
- Major growth observed in digital payments post-2020.
- Smaller cities and rural areas show rising digital adoption.
