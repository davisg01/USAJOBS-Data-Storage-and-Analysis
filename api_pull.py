import requests
import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="######", # Use your own MySQL database password
    database="usa_jobs"
)
cursor = db.cursor()

url = "https://data.usajobs.gov/api/search"
headers = {
    "User-Agent": "######",  # Use the email you registered with for the API
    "Authorization-Key": "########"      # Replace with your actual API key
}

results_per_page = 100
current_page = 1
total_results = 0

# Loop to fetch pages of results
while total_results < 5000:
    params = {
        "Page": current_page,
        "ResultsPerPage": results_per_page,
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        job_listings = data.get("SearchResult", {}).get("SearchResultItems", [])

        if not job_listings:
            break

        for job in job_listings:
            job_details = job.get("MatchedObjectDescriptor", {})
            job_id = job_details.get("PositionID")
            title = job_details.get("PositionTitle")
            organization = job_details.get("OrganizationName")
            department_name = job_details.get("DepartmentName")
            url_name = job_details.get('PositionURI')

            for location in job_details.get("PositionLocation",[]):
                location_name = location.get("LocationName", "").lower()
                work_type_name = "remote job" if 'remote job' in location_name else 'in person'

            salary_min = float(job_details.get("PositionRemuneration", [{}])[0].get("MinimumRange", 0))  # float
            salary_max = float(job_details.get("PositionRemuneration", [{}])[0].get("MaximumRange", 0))  # float

            remuneration_data = job_details.get("PositionRemuneration", [{}])[0]
            pay_type_name = remuneration_data.get("Description", "").lower()

            salary_avg = (salary_min+salary_max)/2
            start_date_name = job_details.get("PositionStartDate", "")
            start_date_name = start_date_name.split("T")[0] if "T" in start_date_name else start_date_name


            for x in job_details.get('PositionLocation'):
                country_name = x.get("CountryCode")
                state_part = x.get("CountrySubDivisionCode")
                city_part = x.get("CityName")
                if country_name == "United States" and state_part:
                    state_city_name = state_part
                elif country_name != "United States" and city_part:
                    state_city_name = city_part.split(", ")[0]
                else:
                    state_city_name = 'Any State'

            print(f"job_id: {job_id} (type: {type(job_id)})")
            print(f"title: {title} (type: {type(title)})")
            print(f"organization: {organization} (type: {type(organization)})")
            print(f"department_name: {department_name} (type: {type(department_name)})")
            print(f"country_name: { country_name} (type: {type( country_name)})")
            print(f" state_city_name: {state_city_name} (type: {type(state_city_name)})")
            print(f"url_name: {url_name} (type: {type(url_name)})")
            print(f"work_type_name: {work_type_name} (type: {type(work_type_name)})")
            print(f"salary_min: {salary_min} (type: {type(salary_min)})")
            print(f"salary_max: {salary_max} (type: {type(salary_max)})")
            print(f"salary_avg: {salary_avg} (type: {type(salary_avg)})")
            print(f"pay_type_name: {pay_type_name} (type: {type(pay_type_name)})")
            print(f"start_date_name: {start_date_name} (type: {type(start_date_name)})")

            insert_query = """
                INSERT INTO job_details (job_id, job_title, organization_name, department, country, region, url, work_type, salary_min, salary_max, salary_avg, pay_type, date_posted)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    job_title = VALUES(job_title),
                    organization_name = VALUES(organization_name)
            """

            cursor.execute(insert_query, (job_id, title, organization, department_name, country_name, state_city_name, url_name, work_type_name, salary_min,
                                          salary_max, salary_avg, pay_type_name, start_date_name))
            total_results += 1  # Increment for each job added


        db.commit()

        # Move to the next page
        current_page += 1
    else:
        print(f"Error: {response.status_code}")
        break
cursor.close()
db.close()

print(f"Total jobs retrieved and stored: {total_results}")




