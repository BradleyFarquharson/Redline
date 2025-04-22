FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY redline/ redline/
COPY config/ config/
EXPOSE 8501
CMD ["streamlit", "run", "redline/app.py", "--server.headless=true", "--server.port=8501"] 