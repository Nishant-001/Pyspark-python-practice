import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote


# ============================================================
# CONFIG
# ============================================================

SEARCHES = [
    "AWS Data Engineer",
    "Data Engineer",
    "PySpark Data Engineer",
    "Big Data Engineer",
    "Python Data Engineer",
    "Spark Data Engineer"
]

LOCATION = "India"

MAX_PAGES_PER_SEARCH = 10
SLEEP_SECONDS = 2

OUTPUT_FILE = "jobs_last_24h_with_email.csv"


# ============================================================
# SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/154.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9"
})


# ============================================================
# EMAIL REGEX
# ============================================================

EMAIL_REGEX = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)


def extract_emails(text):

    if not text:
        return []

    emails = EMAIL_REGEX.findall(text)

    # Remove duplicates
    emails = list(dict.fromkeys(
        email.lower() for email in emails
    ))

    return emails


# ============================================================
# CONVERT LINKEDIN "X HOURS AGO"
# ============================================================

def is_within_24_hours(posted_text):

    if not posted_text:
        return False

    text = posted_text.lower().strip()

    # Examples:
    # 5 minutes ago
    # 22 hours ago
    # 1 day ago
    # 2 days ago

    match = re.search(
        r"(\d+)\s+(minute|minutes|hour|hours|day|days)",
        text
    )

    if not match:
        return False

    number = int(match.group(1))
    unit = match.group(2)

    if "minute" in unit:
        return True

    if "hour" in unit:
        return number <= 24

    if "day" in unit:
        return number == 0

    return False


# ============================================================
# GET JOB DETAILS
# ============================================================

def get_job_details(job_id):

    url = (
        "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/"
        f"{job_id}"
    )

    try:

        response = session.get(
            url,
            timeout=20
        )

        if response.status_code != 200:
            print(
                f"    Job {job_id}: HTTP {response.status_code}"
            )
            return None

        soup = BeautifulSoup(
            response.text,
            "html.parser"
        )

        # ----------------------------------------------------
        # TITLE
        # ----------------------------------------------------

        title_element = soup.find(
            "h2",
            class_=lambda x: x and "top-card-layout__title" in x
        )

        if not title_element:
            title_element = soup.find("h2")

        title = (
            title_element.get_text(" ", strip=True)
            if title_element
            else ""
        )

        # ----------------------------------------------------
        # COMPANY
        # ----------------------------------------------------

        company_element = soup.find(
            "a",
            class_=lambda x: x and "topcard__org-name-link" in x
        )

        if not company_element:
            company_element = soup.find(
                "a",
                class_=lambda x: x and "topcard__org-name" in x
            )

        company = (
            company_element.get_text(" ", strip=True)
            if company_element
            else ""
        )

        # ----------------------------------------------------
        # LOCATION
        # ----------------------------------------------------

        location_element = soup.find(
            "span",
            class_=lambda x: x and "topcard__flavor--bullet" in x
        )

        location = (
            location_element.get_text(" ", strip=True)
            if location_element
            else ""
        )

        # ----------------------------------------------------
        # POSTED TIME
        # ----------------------------------------------------

        posted_element = soup.find(
            "span",
            class_=lambda x: x and "posted-date" in x
        )

        if not posted_element:

            posted_element = soup.find(
                "time"
            )

        posted = (
            posted_element.get_text(" ", strip=True)
            if posted_element
            else ""
        )

        # ----------------------------------------------------
        # DESCRIPTION
        # ----------------------------------------------------

        description_element = soup.find(
            "div",
            class_=lambda x: x and "description__text" in x
        )

        if not description_element:

            description_element = soup.find(
                "div",
                class_=lambda x: x and "description" in x
            )

        description = (
            description_element.get_text(
                "\n",
                strip=True
            )
            if description_element
            else ""
        )

        return {
            "job_id": job_id,
            "title": title,
            "company": company,
            "location": location,
            "posted": posted,
            "description": description,
            "job_url": (
                f"https://www.linkedin.com/jobs/view/{job_id}"
            )
        }

    except Exception as e:

        print(
            f"    Error getting job {job_id}: {e}"
        )

        return None


# ============================================================
# SEARCH LINKEDIN
# ============================================================

def search_jobs(keyword):

    print()
    print("=" * 70)
    print(f"SEARCHING: {keyword}")
    print("=" * 70)

    jobs = []

    for page in range(MAX_PAGES_PER_SEARCH):

        start = page * 25

        url = (
            "https://www.linkedin.com/jobs-guest/jobs/api/"
            "seeMoreJobPostings/search?"
            f"keywords={quote(keyword)}&"
            f"location={quote(LOCATION)}&"
            f"start={start}&"
            "f_TPR=r86400"
        )

        print(
            f"\nPage {page + 1} | start={start}"
        )

        try:

            response = session.get(
                url,
                timeout=20
            )

            if response.status_code != 200:

                print(
                    f"Search failed: HTTP "
                    f"{response.status_code}"
                )

                break

            soup = BeautifulSoup(
                response.text,
                "html.parser"
            )

            cards = soup.find_all(
                "li"
            )

            if not cards:

                print("No more jobs.")
                break

            found_on_page = 0

            for card in cards:

                # ------------------------------------------------
                # JOB ID
                # ------------------------------------------------

                entity = card.get(
                    "data-entity-urn"
                )

                if not entity:

                    entity_element = card.find(
                        attrs={
                            "data-entity-urn": True
                        }
                    )

                    if entity_element:
                        entity = entity_element.get(
                            "data-entity-urn"
                        )

                if not entity:
                    continue

                match = re.search(
                    r"jobPosting:(\d+)",
                    entity
                )

                if not match:
                    continue

                job_id = match.group(1)

                # ------------------------------------------------
                # POSTED TIME FROM SEARCH CARD
                # ------------------------------------------------

                time_element = card.find(
                    "time"
                )

                posted = (
                    time_element.get_text(
                        " ",
                        strip=True
                    )
                    if time_element
                    else ""
                )

                # ------------------------------------------------
                # STRICT 24-HOUR CHECK
                # ------------------------------------------------

                if not is_within_24_hours(
                    posted
                ):
                    continue

                found_on_page += 1

                print(
                    f"  {job_id} | {posted}"
                )

                # ------------------------------------------------
                # FULL JOB
                # ------------------------------------------------

                job = get_job_details(
                    job_id
                )

                if not job:
                    continue

                # Use detailed page posted value if available
                if not job["posted"]:
                    job["posted"] = posted

                # ------------------------------------------------
                # EMAIL EXTRACTION
                # ------------------------------------------------

                emails = extract_emails(
                    job["description"]
                )

                if not emails:

                    print(
                        "      ❌ No email"
                    )

                    continue

                # ------------------------------------------------
                # EMAIL FOUND
                # ------------------------------------------------

                job["emails"] = ", ".join(
                    emails
                )

                job["search_keyword"] = keyword

                print(
                    f"      ✅ EMAIL: "
                    f"{job['emails']}"
                )

                jobs.append(job)

                time.sleep(
                    SLEEP_SECONDS
                )

            print(
                f"  Jobs within 24h on page: "
                f"{found_on_page}"
            )

            time.sleep(
                SLEEP_SECONDS
            )

        except Exception as e:

            print(
                f"Search error: {e}"
            )

            break

    return jobs


# ============================================================
# MAIN
# ============================================================

all_jobs = []

for keyword in SEARCHES:

    results = search_jobs(
        keyword
    )

    all_jobs.extend(
        results
    )


# ============================================================
# REMOVE DUPLICATES
# ============================================================

unique_jobs = {}

for job in all_jobs:

    unique_jobs[
        job["job_id"]
    ] = job


all_jobs = list(
    unique_jobs.values()
)


# ============================================================
# DATAFRAME
# ============================================================

if all_jobs:

    df = pd.DataFrame(
        all_jobs
    )

    df = df[
        [
            "job_id",
            "title",
            "company",
            "location",
            "posted",
            "emails",
            "job_url",
            "search_keyword",
            "description"
        ]
    ]

    df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print()
    print("=" * 70)
    print("FINAL RESULTS")
    print("=" * 70)

    print(
        f"Jobs with email: {len(df)}"
    )

    print(
        f"Saved to: {OUTPUT_FILE}"
    )

    print()

    print(
        df[
            [
                "title",
                "company",
                "location",
                "posted",
                "emails",
                "job_url"
            ]
        ].to_string(
            index=False
        )
    )

else:

    print()
    print(
        "No jobs posted within the last 24 hours "
        "with an email address were found."
    )