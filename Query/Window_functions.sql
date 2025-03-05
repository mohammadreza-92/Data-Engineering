-- SQL File: Window Functions, Cumulative Sum, and Rolling Sum Examples
-- This file contains examples and explanations for window functions, cumulative sums, and rolling sums.

-----------------------------------------------------------
-- 1. Window Functions: RANK, DENSE_RANK, and ROW_NUMBER --
-----------------------------------------------------------

/*
Window functions are used to perform calculations across a set of rows related to the current row.
Common use cases include finding the "second highest salary" or ranking rows.
*/

-- Example: Find the second highest salary using RANK
WITH ranked_salaries AS (
    SELECT 
        employee_id, 
        salary,
        RANK() OVER (ORDER BY salary DESC) AS salary_rank
    FROM 
        employees
)
SELECT 
    employee_id, 
    salary
FROM 
    ranked_salaries
WHERE 
    salary_rank = 2;

/*
Explanation:
- RANK() assigns a unique rank to each row. If two rows have the same value, they get the same rank, and the next rank is skipped.
- The query finds the employee with the second highest salary.
*/

-- Example: Find the second highest salary using DENSE_RANK
WITH dense_ranked_salaries AS (
    SELECT 
        employee_id, 
        salary,
        DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_dense_rank
    FROM 
        employees
)
SELECT 
    employee_id, 
    salary
FROM 
    dense_ranked_salaries
WHERE 
    salary_dense_rank = 2;

/*
Explanation:
- DENSE_RANK() is similar to RANK, but it does not skip ranks if there are ties.
- The query finds the employee with the second highest salary.
*/

-- Example: Find the second highest salary using ROW_NUMBER
WITH row_numbered_salaries AS (
    SELECT 
        employee_id, 
        salary,
        ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num
    FROM 
        employees
)
SELECT 
    employee_id, 
    salary
FROM 
    row_numbered_salaries
WHERE 
    row_num = 2;

/*
Explanation:
- ROW_NUMBER() assigns a unique number to each row, ignoring ties.
- The query finds the employee with the second highest salary.
*/

-- Example: Use PARTITION BY to find the second highest salary in each department
WITH ranked_salaries_by_dept AS (
    SELECT 
        employee_id, 
        department_id,
        salary,
        RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank
    FROM 
        employees
)
SELECT 
    employee_id, 
    department_id,
    salary
FROM 
    ranked_salaries_by_dept
WHERE 
    salary_rank = 2;

/*
Explanation:
- PARTITION BY divides the result set into partitions (e.g., by department).
- The query finds the second highest salary in each department.
*/

-----------------------------------------------------------
-- 2. Self-Join vs LAG/LEAD Window Functions --
-----------------------------------------------------------

/*
Self-joins and LAG/LEAD window functions can solve similar problems, but LAG/LEAD is more performant and easier to read.
However, some environments (e.g., Facebook/Meta) require ANSI SQL, so you may need to replicate LAG/LEAD using self-joins.
*/

-- Example: Find the second highest salary using a self-join
SELECT 
    e1.employee_id, 
    e1.salary
FROM 
    employees e1
LEFT JOIN 
    employees e2
ON 
    e1.salary < e2.salary
GROUP BY 
    e1.employee_id, e1.salary
HAVING 
    COUNT(DISTINCT e2.salary) = 1;

/*
Explanation:
- A self-join is used to compare rows within the same table.
- The query finds the second highest salary by counting distinct higher salaries.
*/

-- Example: Find the second highest salary using LAG
WITH ranked_salaries AS (
    SELECT 
        employee_id, 
        salary,
        LAG(salary, 1) OVER (ORDER BY salary DESC) AS next_highest_salary
    FROM 
        employees
)
SELECT 
    employee_id, 
    salary
FROM 
    ranked_salaries
WHERE 
    next_highest_salary IS NOT NULL
ORDER BY 
    salary DESC
LIMIT 1;

/*
Explanation:
- LAG() accesses the value of the previous row in the result set.
- The query finds the second highest salary using LAG.
*/

-- Example: Replicate LAG using a self-join
SELECT 
    e1.employee_id, 
    e1.salary,
    MIN(e2.salary) AS next_highest_salary
FROM 
    employees e1
LEFT JOIN 
    employees e2
ON 
    e1.salary < e2.salary
GROUP BY 
    e1.employee_id, e1.salary
ORDER BY 
    e1.salary DESC;

/*
Explanation:
- This query replicates the functionality of LAG using a self-join.
- It finds the next highest salary for each employee.
*/

-----------------------------------------------------------
-- 3. Rolling and Cumulative Sums --
-----------------------------------------------------------

/*
Rolling and cumulative sums are used to calculate aggregates over a specific window of rows.
- Cumulative Sum: Sum of all rows from the start to the current row.
- Rolling Sum: Sum of a fixed number of preceding rows (e.g., last 30 days).
*/

-- Example: Cumulative Sum
SELECT
    sale_date,
    daily_sales,
    SUM(daily_sales) OVER (
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sum
FROM
    sales
ORDER BY
    sale_date;

/*
Explanation:
- The query calculates the cumulative sum of daily sales from the start to the current row.
- UNBOUNDED PRECEDING includes all rows from the beginning.
*/

-- Example: Rolling 30-Day Sum
SELECT
    sale_date,
    daily_sales,
    SUM(daily_sales) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30_day_sum
FROM
    sales
ORDER BY
    sale_date;

/*
Explanation:
- The query calculates the sum of daily sales for the last 30 days (including the current row).
- 29 PRECEDING includes the 29 rows before the current row.
*/

-- Example: Rolling 30-Day Sum with RANGE (for date gaps)
SELECT
    sale_date,
    daily_sales,
    SUM(daily_sales) OVER (
        ORDER BY sale_date
        RANGE BETWEEN INTERVAL '29' DAY PRECEDING AND CURRENT ROW
    ) AS rolling_30_day_sum
FROM
    sales
ORDER BY
    sale_date;

/*
Explanation:
- The query calculates the sum of daily sales for the last 30 days, even if there are gaps in the data.
- RANGE BETWEEN uses actual date intervals instead of row counts.
*/-------------------------------
