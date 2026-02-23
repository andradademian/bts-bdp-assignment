# Deployment Architecture

- The application is deployed on an AWS EC2 instance (t2.micro, Ubuntu 24.04) and accessible at http://54.87.44.180:8080.
- The API is built with FastAPI and served via Uvicorn. PostgreSQL runs as a Docker container on the same EC2 instance, exposed on port 5432. The FastAPI application connects to PostgreSQL using psycopg2, with the connection string configured via the BDI_DB_URL environment variable (postgresql://postgres:postgres@localhost:5432/hr_database).
- The EC2 security group allows inbound traffic on port 22 (SSH) and port 8080 (API), open to all IPs (0.0.0.0/0).