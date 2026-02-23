from typing import Annotated

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["s5"],
)


def get_connection():
    return psycopg2.connect(settings.db_url)


@s5.post("/db/init")
def init_database() -> str:
    """Create all HR database tables using the official hr_schema.sql."""
    schema_sql = """
    DROP TABLE IF EXISTS salary_history CASCADE;
    DROP TABLE IF EXISTS employee_project CASCADE;
    DROP TABLE IF EXISTS project CASCADE;
    DROP TABLE IF EXISTS employee CASCADE;
    DROP TABLE IF EXISTS department CASCADE;

    CREATE TABLE department (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL UNIQUE,
        location VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE employee (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE,
        hire_date DATE NOT NULL,
        salary DECIMAL(10, 2) NOT NULL,
        department_id INTEGER REFERENCES department(id) ON DELETE SET NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE project (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        start_date DATE NOT NULL,
        end_date DATE,
        budget DECIMAL(12, 2),
        department_id INTEGER REFERENCES department(id) ON DELETE SET NULL
    );

    CREATE TABLE employee_project (
        employee_id INTEGER REFERENCES employee(id) ON DELETE CASCADE,
        project_id INTEGER REFERENCES project(id) ON DELETE CASCADE,
        role VARCHAR(50) DEFAULT 'member',
        assigned_date DATE DEFAULT CURRENT_DATE,
        PRIMARY KEY (employee_id, project_id)
    );

    CREATE TABLE salary_history (
        id SERIAL PRIMARY KEY,
        employee_id INTEGER NOT NULL REFERENCES employee(id) ON DELETE CASCADE,
        old_salary DECIMAL(10, 2) NOT NULL,
        new_salary DECIMAL(10, 2) NOT NULL,
        change_date DATE NOT NULL DEFAULT CURRENT_DATE,
        reason VARCHAR(200)
    );

    CREATE INDEX idx_employee_department ON employee(department_id);
    CREATE INDEX idx_employee_email ON employee(email);
    CREATE INDEX idx_project_department ON project(department_id);
    CREATE INDEX idx_salary_history_employee ON salary_history(employee_id);
    CREATE INDEX idx_employee_project_employee ON employee_project(employee_id);
    CREATE INDEX idx_employee_project_project ON employee_project(project_id);
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
    return "OK"


@s5.post("/db/seed")
def seed_database() -> str:
    """Populate the HR database with the official hr_seed_data.sql sample data."""
    seed_sql = """
    INSERT INTO department (name, location) VALUES
        ('Engineering', 'Barcelona'),
        ('Marketing', 'Madrid'),
        ('Human Resources', 'Barcelona'),
        ('Finance', 'London'),
        ('Sales', 'New York');

    INSERT INTO employee (first_name, last_name, email, hire_date, salary, department_id) VALUES
        ('Anna', 'Garcia', 'anna.garcia@company.com', '2020-03-15', 55000.00, 1),
        ('Marc', 'Lopez', 'marc.lopez@company.com', '2019-07-01', 62000.00, 1),
        ('Laura', 'Martinez', 'laura.martinez@company.com', '2021-01-10', 48000.00, 2),
        ('Carlos', 'Fernandez', 'carlos.fernandez@company.com', '2018-11-20', 70000.00, 1),
        ('Sofia', 'Rodriguez', 'sofia.rodriguez@company.com', '2022-06-01', 45000.00, 3),
        ('David', 'Sanchez', 'david.sanchez@company.com', '2020-09-15', 58000.00, 4),
        ('Maria', 'Diaz', 'maria.diaz@company.com', '2017-04-01', 75000.00, 4),
        ('Pablo', 'Ruiz', 'pablo.ruiz@company.com', '2023-02-01', 42000.00, 5),
        ('Elena', 'Torres', 'elena.torres@company.com', '2021-08-15', 52000.00, 2),
        ('Jorge', 'Navarro', 'jorge.navarro@company.com', '2019-12-01', 60000.00, 1),
        ('Clara', 'Moreno', 'clara.moreno@company.com', '2022-03-15', 47000.00, 5),
        ('Ivan', 'Jimenez', 'ivan.jimenez@company.com', '2020-05-20', 53000.00, 1);

    INSERT INTO project (name, description, start_date, end_date, budget, department_id) VALUES
        ('Data Platform', 'Build the internal data platform', '2024-01-15', '2024-12-31', 150000.00, 1),
        ('Brand Refresh', 'Company rebranding campaign', '2024-03-01', '2024-09-30', 80000.00, 2),
        ('HR Portal', 'Employee self-service portal', '2024-02-01', NULL, 60000.00, 3),
        ('Q4 Budget', 'Annual budget planning', '2024-07-01', '2024-10-31', 25000.00, 4),
        ('Mobile App', 'Customer mobile application', '2024-04-15', NULL, 200000.00, 1),
        ('Sales Dashboard', 'Real-time sales analytics', '2024-05-01', '2024-11-30', 45000.00, 5);

    INSERT INTO employee_project (employee_id, project_id, role, assigned_date) VALUES
        (1, 1, 'developer', '2024-01-15'),
        (2, 1, 'lead', '2024-01-15'),
        (4, 1, 'architect', '2024-01-15'),
        (10, 1, 'developer', '2024-02-01'),
        (12, 1, 'developer', '2024-03-01'),
        (3, 2, 'coordinator', '2024-03-01'),
        (9, 2, 'designer', '2024-03-15'),
        (5, 3, 'lead', '2024-02-01'),
        (1, 5, 'developer', '2024-04-15'),
        (2, 5, 'lead', '2024-04-15'),
        (4, 5, 'architect', '2024-04-15'),
        (12, 5, 'developer', '2024-05-01'),
        (6, 4, 'analyst', '2024-07-01'),
        (7, 4, 'lead', '2024-07-01'),
        (8, 6, 'developer', '2024-05-01'),
        (11, 6, 'analyst', '2024-05-15');

    INSERT INTO salary_history (employee_id, old_salary, new_salary, change_date, reason) VALUES
        (1, 48000.00, 52000.00, '2021-03-15', 'Annual review'),
        (1, 52000.00, 55000.00, '2023-03-15', 'Promotion'),
        (2, 55000.00, 58000.00, '2020-07-01', 'Annual review'),
        (2, 58000.00, 62000.00, '2022-07-01', 'Promotion to lead'),
        (4, 60000.00, 65000.00, '2020-11-20', 'Annual review'),
        (4, 65000.00, 70000.00, '2022-11-20', 'Promotion to architect'),
        (7, 65000.00, 70000.00, '2019-04-01', 'Annual review'),
        (7, 70000.00, 75000.00, '2021-04-01', 'Promotion'),
        (6, 50000.00, 54000.00, '2021-09-15', 'Annual review'),
        (6, 54000.00, 58000.00, '2023-09-15', 'Annual review'),
        (10, 52000.00, 56000.00, '2021-12-01', 'Annual review'),
        (10, 56000.00, 60000.00, '2023-12-01', 'Promotion');
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(seed_sql)
    return "OK"


@s5.get("/departments/")
def list_departments() -> list[dict]:
    """Return all departments: id, name, location."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name, location FROM department ORDER BY id")
            return [dict(row) for row in cur.fetchall()]


@s5.get("/employees/")
def list_employees(
    page: Annotated[int, Query(description="Page number (1-indexed)", ge=1)] = 1,
    per_page: Annotated[int, Query(description="Number of employees per page", ge=1, le=100)] = 10,
) -> list[dict]:
    """Return employees with department name, paginated."""
    offset = (page - 1) * per_page
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT e.id, e.first_name, e.last_name, e.email,
                       e.salary, d.name AS department_name
                FROM employee e
                LEFT JOIN department d ON e.department_id = d.id
                ORDER BY e.id
                LIMIT %s OFFSET %s
                """,
                (per_page, offset),
            )
            rows = cur.fetchall()
            return [
                {
                    "id": row["id"],
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "email": row["email"],
                    "salary": float(row["salary"]),
                    "department_name": row["department_name"],
                }
                for row in rows
            ]


@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    """Return all employees in a specific department."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, first_name, last_name, email, salary, hire_date
                FROM employee
                WHERE department_id = %s
                ORDER BY id
                """,
                (dept_id,),
            )
            rows = cur.fetchall()
            return [
                {
                    "id": row["id"],
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "email": row["email"],
                    "salary": float(row["salary"]),
                    "hire_date": str(row["hire_date"]),
                }
                for row in rows
            ]


@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    """Return KPIs for a department: name, employee_count, avg_salary, project_count."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    d.name AS department_name,
                    COUNT(DISTINCT e.id)  AS employee_count,
                    AVG(e.salary)         AS avg_salary,
                    COUNT(DISTINCT p.id)  AS project_count
                FROM department d
                LEFT JOIN employee e ON e.department_id = d.id
                LEFT JOIN project  p ON p.department_id = d.id
                WHERE d.id = %s
                GROUP BY d.name
                """,
                (dept_id,),
            )
            row = cur.fetchone()
            if not row:
                return {}
            return {
                "department_name": row["department_name"],
                "employee_count": row["employee_count"],
                "avg_salary": float(row["avg_salary"]) if row["avg_salary"] else 0.0,
                "project_count": row["project_count"],
            }


@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    """Return salary evolution for an employee, ordered by change_date ascending."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT change_date, old_salary, new_salary, reason
                FROM salary_history
                WHERE employee_id = %s
                ORDER BY change_date ASC
                """,
                (emp_id,),
            )
            rows = cur.fetchall()
            return [
                {
                    "change_date": str(row["change_date"]),
                    "old_salary": float(row["old_salary"]),
                    "new_salary": float(row["new_salary"]),
                    "reason": row["reason"],
                }
                for row in rows
            ]