CREATE DATABASE  as_2;
USE as_2;

CREATE TABLE employees (
    employee_id INT PRIMARY KEY AUTO_INCREMENT,
    full_name VARCHAR(100) NOT NULL,
    team VARCHAR(50),
    hire_date DATE NOT NULL
);

CREATE TABLE calls (
    call_id INT PRIMARY KEY AUTO_INCREMENT,
    employee_id INT NOT NULL,
    call_time DATETIME NOT NULL,
    phone VARCHAR(20),
    direction ENUM('inbound','outbound'),
    status ENUM('completed','missed','failed'),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

INSERT INTO employees (full_name, team, hire_date)
SELECT
    CONCAT('Employee_', seq) AS full_name,
    ELT(FLOOR(1 + RAND()*3), 'Support', 'Sales', 'Technical') AS team,
    DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*1000) DAY)
FROM (
    SELECT @row := @row + 1 AS seq
    FROM
        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) t1,
        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) t2,
        (SELECT @row:=0) r
    LIMIT 50
) seqs;

INSERT INTO calls (employee_id, call_time, phone, direction, status)
SELECT
    e.employee_id,
    NOW() - INTERVAL FLOOR(RAND()*60) MINUTE,
    CONCAT('+1-555-', FLOOR(1000 + RAND()*9000)),
    ELT(FLOOR(1 + RAND()*2), 'inbound','outbound'),
    ELT(FLOOR(1 + RAND()*3), 'completed','missed','failed')
FROM (
    SELECT employee_id
    FROM employees
    ORDER BY RAND()
    LIMIT 5
) e;

SELECT * FROM employees;
SELECT * FROM calls;
SELECT call_id FROM calls ORDER BY call_id;