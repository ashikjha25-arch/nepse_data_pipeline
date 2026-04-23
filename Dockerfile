# Use a lightweight Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1

# Show logs directly in terminal
ENV PYTHONUNBUFFERED=1

# Set working directory inside container
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy full project
COPY . .

# Default command
CMD ["uvicorn", "connection.main_conn:app", "--host", "0.0.0.0", "--port", "8000"]