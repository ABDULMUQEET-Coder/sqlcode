-- SUM of call duration per each employee
SELECT 
    employee.id,
    employee.first_name,
    employee.last_name,
    SUM(DATEDIFF("SECOND", call.start_time, call.end_time)) AS call_duration_sum
FROM call
INNER JOIN employee ON call.employee_id = employee.id
GROUP BY
    employee.id,
    employee.first_name,
    employee.last_name
ORDER BY
    employee.id ASC;  
