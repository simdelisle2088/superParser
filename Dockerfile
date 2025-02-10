# Use Python 3.12.3 on Alpine 3.19 as the base image
FROM python:3.12.3-alpine3.19

# Install system dependencies required for some Python packages
RUN apk add --no-cache \
    gcc \
    musl-dev \
    python3-dev \
    libffi-dev \
    openssl-dev

# Set the working directory in the container to /app
WORKDIR /app

# Copy the Python dependency file and all necessary application files to /app
COPY requirements.txt *.py .env /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables for configuration
ENV HOST="127.0.0.1"
ENV PORT=8013
# ENV PORT=8014
ENV LOOP=uvloop
ENV HTTP=httptools
ENV LOG_LEVEL=debug

# Expose the port the app runs on
# EXPOSE 8013
EXPOSE 8014

# Command to run the application using Uvicorn
CMD uvicorn main:app --host $HOST --port $PORT --loop $LOOP --http $HTTP --log-level $LOG_LEVEL