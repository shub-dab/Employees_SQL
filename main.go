package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

type Employee struct {
	ID         string `json:"id"`
	Firstname  string `json:"firstname"`
	Lastname   string `json:"lastname"`
}

var db *sql.DB

var cfg = mysql.Config{
	User:   "root",
	Passwd: "abcd",
	Net:    "tcp",
	Addr:   "127.0.0.1:3306",
	DBName: "employee",
}

func getMySQLDB() *sql.DB {
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func createEmployee(w http.ResponseWriter, r *http.Request)  {
	w.Header().Set("Content-Type", "application/json")
	db = getMySQLDB()
	defer db.Close()
	employee := Employee{}
	json.NewDecoder(r.Body).Decode(&employee)
	
	result, err := db.Exec("insert into employee(ID, Firstname, Lastname) values (?, ?, ?)", employee.ID, employee.Firstname, employee.Lastname)

	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	} else {
		_, err := result.LastInsertId()
		if err != nil {
			json.NewEncoder(w).Encode("{error:record not inserted}")
		} else {
			json.NewEncoder(w).Encode(employee)
		}
	}
}

func getEmployee(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	db = getMySQLDB()
	employee := Employee{}
	rows, err := db.Query("select * from employee where ID=?", params["id"])
	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	} else {
		for rows.Next() {
			rows.Scan(&employee.ID, &employee.Firstname, &employee.Lastname)
		}
		json.NewEncoder(w).Encode(employee)
	}
}

func getAllEmployees(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	db = getMySQLDB()
	employees := []Employee{}
	employee := Employee{}
	rows, err := db.Query("select * from employee")
	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	} else {
		for rows.Next() {
			rows.Scan(&employee.ID, &employee.Firstname, &employee.Lastname)
			employees = append(employees, employee)
		}
		json.NewEncoder(w).Encode(employees)
	}
}

func deleteEmployee(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	db = getMySQLDB()
	defer db.Close()
	params := mux.Vars(r)
	result, err := db.Exec("delete from employee where ID=?", params["id"]) 
	_, _ = db.Exec("delete from employee_department_mapping where EmpID=?", params["id"]) 
	_, _ = db.Exec("delete from employee_job_mapping where EmpID=?", params["id"]) 
	if err != nil { 
		fmt.Fprintf(w, ""+err.Error()) 
	} else {
		_, err := result.RowsAffected()
		if err != nil {
			json.NewEncoder(w).Encode("{result:Record is not deleted}")
		} else {
			json.NewEncoder(w).Encode("result:Record is deleted")
		}
	}
}

func assignDepartment(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	db = getMySQLDB()
	defer db.Close()

	params := mux.Vars(r)
	_, err := db.Exec("insert into employee_department_mapping (EmpID, Department) values (?, ?)", params["id"], params["dep"])
	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	}

	rows, err := db.Query("select * from employee_department_mapping where EmpID=?", params["id"])
	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	} else { 
		var id, department string
		for rows.Next() { 
			rows.Scan(&id, &department)
		} 
		json.NewEncoder(w).Encode(fmt.Sprintf("EmpID = %s, Department = %s", id, department))
	} 
} 

func assignJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	db = getMySQLDB()
	defer db.Close()

	params := mux.Vars(r)
	_, err := db.Exec("insert into employee_job_mapping (EmpID, Job, JobStatus) values (?, ?, ?)", params["id"], params["job"], "Pending")
	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	}
	rows, err := db.Query("select * from employee_job_mapping where EmpID=?", params["id"])
	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	} else {
		var id, job, jobStatus string
		for rows.Next() {
			rows.Scan(&id, &job, &jobStatus)
		}
		json.NewEncoder(w).Encode(fmt.Sprintf("EmpID = %s, Job = %s, JobStatus = %s", id, job, jobStatus))
	}
}

func makeJobCompleted(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	db = getMySQLDB()
	defer db.Close()

	params := mux.Vars(r)
	_, err := db.Exec("update employee_job_mapping set JobStatus=? where EmpID=?", "Completed", params["id"])
	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	}
	rows, err := db.Query("select * from employee_job_mapping where EmpID=?", params["id"])
	if err != nil {
		fmt.Fprintf(w, ""+err.Error())
	} else {
		var id, job, jobStatus string
		for rows.Next() {
			rows.Scan(&id, &job, &jobStatus)
		}
		json.NewEncoder(w).Encode(fmt.Sprintf("EmpID = %s, Job = %s, JobStatus = %s", id, job, jobStatus))
	}
}

func getJobStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	db = getMySQLDB()
	defer db.Close()

	params := mux.Vars(r)

	var jobStatus string
	rows, err := db.Query("select JobStatus from employee_job_mapping where EmpID=?", params["id"])
	if err != nil {
		fmt.Fprintf(w, "" + err.Error())
	} else {
		for rows.Next() {
			rows.Scan(&jobStatus)
		}
		result := fmt.Sprintf("Job Status = %s for EmpID = %s", jobStatus, params["id"])
		json.NewEncoder(w).Encode(result)
	}
}

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/api/employees", createEmployee).Methods("POST")
	r.HandleFunc("/api/employees/{id}", getEmployee).Methods("GET")
	r.HandleFunc("/api/employees", getAllEmployees).Methods("GET")
	r.HandleFunc("/api/employees/department/{id}/{dep}", assignDepartment).Methods("PATCH")
	r.HandleFunc("/api/employees/job/{id}/{job}", assignJob).Methods("PATCH")
	r.HandleFunc("/api/employees/jobcompleted/{id}", makeJobCompleted).Methods("PATCH")
	r.HandleFunc("/api/employees/jobstatus/{id}", getJobStatus).Methods("GET")
	r.HandleFunc("/api/employees/{id}", deleteEmployee).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":5000", r))
}
