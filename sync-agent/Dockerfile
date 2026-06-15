FROM python:3.12-alpine
RUN adduser -D appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY agent ./agent
USER appuser
CMD ["python3", "-u", "-m", "agent.main"]
