import json
import hashlib
import requests
from datetime import datetime

from src.storage.minio.safaa_client import MinIOClient


# ==============================================================
# CONFIGURATION ORCID
# ==============================================================

SOURCE_NAME = "orcid"
SOURCE_SYSTEM = "orcid_api"

SEARCH_QUERY = "data science"
MAX_RECORDS = 20


# ==============================================================
# COMMON METADATA FUNCTIONS
# ==============================================================

def generate_record_id(source_system: str, source_url: str, data: dict) -> str:
    """Generate a stable record_id for traceability."""
    content_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    hash_obj = hashlib.sha256(content_str.encode("utf-8"))
    return f"{source_system}_{hash_obj.hexdigest()[:16]}"


def create_common_fields(source_system: str, source_url: str, data: dict) -> dict:
    """Add common metadata fields useful for curated/Hudi tables."""
    clean_data = {
        k: v for k, v in data.items()
        if k not in ["record_id", "source_system", "source_url"]
    }

    content_hash = hashlib.sha256(
        json.dumps(clean_data, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()

    now = datetime.now().isoformat()

    return {
        "record_id": generate_record_id(source_system, source_url, clean_data),
        "source_system": source_system,
        "source_url": source_url,
        "content_hash": content_hash,
        "crawl_timestamp": now,
        "business_timestamp": now,
        "is_deleted": False,
        "language": "en",
        **data
    }


def get_date_partition() -> dict:
    """Return date partition fields for MinIO paths."""
    now = datetime.now()

    return {
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "timestamp": now.strftime("%Y%m%d_%H%M%S"),
        "iso": now.isoformat()
    }


def safe_value(obj, *keys, default=""):
    """Safely read nested dictionaries."""
    current = obj

    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)

    if current is None:
        return default

    return current


# ==============================================================
# ORCID API EXTRACTION
# ==============================================================

def search_orcid_ids(query: str, limit: int = 20):
    """
    Search public ORCID records and return ORCID IDs.
    This avoids using a personal ORCID account manually.
    """

    url = "https://pub.orcid.org/v3.0/expanded-search/"

    headers = {
        "Accept": "application/vnd.orcid+json",
        "User-Agent": "UniversityDataPlatform/1.0"
    }

    params = {
        "q": query,
        "rows": limit
    }

    print(f"ORCID search query: {query}")
    print(f"Requested ORCID IDs: {limit}")

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    search_data = response.json()
    ids = []

    # expanded-search format
    for item in search_data.get("expanded-result", []):
        orcid_id = item.get("orcid-id")
        if orcid_id:
            ids.append(orcid_id)

    # fallback for standard search format
    if not ids:
        for item in search_data.get("result", []):
            orcid_id = safe_value(item, "orcid-identifier", "path")
            if orcid_id:
                ids.append(orcid_id)

    ids = list(dict.fromkeys(ids))

    print(f"ORCID IDs found: {len(ids)}")

    return ids[:limit], search_data


def extract_orcid_record(orcid_id: str):
    """Fetch one public ORCID record."""

    url = f"https://pub.orcid.org/v3.0/{orcid_id}/record"

    headers = {
        "Accept": "application/vnd.orcid+json",
        "User-Agent": "UniversityDataPlatform/1.0"
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=30
    )

    response.raise_for_status()

    return response.json(), response.status_code


def extract_orcid_records(query: str = SEARCH_QUERY, limit: int = MAX_RECORDS):
    """
    Search ORCID IDs, then fetch their public records.
    """

    orcid_ids, search_raw = search_orcid_ids(
        query=query,
        limit=limit
    )

    records = []
    errors = []

    print(f"ORCID extraction started ({len(orcid_ids)} records)")

    for orcid_id in orcid_ids:
        try:
            record, status = extract_orcid_record(orcid_id)

            records.append({
                "orcid_id": orcid_id,
                "source_url": f"https://orcid.org/{orcid_id}",
                "http_status": status,
                "record": record
            })

            print(f"OK ORCID | {orcid_id}")

        except Exception as e:
            print(f"ERROR ORCID | {orcid_id} | {e}")
            errors.append({
                "orcid_id": orcid_id,
                "source_url": f"https://orcid.org/{orcid_id}",
                "error": str(e)
            })

    data = {
        "source": SOURCE_NAME,
        "source_type": "api",
        "query": query,
        "requested_limit": limit,
        "records_count": len(records),
        "errors_count": len(errors),
        "search_raw": search_raw,
        "records": records,
        "errors": errors
    }

    return data, 200 if records else 500


# ==============================================================
# TRANSFORMATION TO HUDI-READY STRUCTURES
# This is not the final Spark/Hudi write.
# It only prepares clean JSON records for the next stage.
# ==============================================================

def extract_keywords(person: dict) -> list:
    keywords = []

    for item in safe_value(person, "keywords", "keyword", default=[]):
        content = item.get("content")
        if content:
            keywords.append(content)

    return keywords


def extract_researcher_urls(person: dict) -> list:
    urls = []

    for item in safe_value(person, "researcher-urls", "researcher-url", default=[]):
        url_name = item.get("url-name", "")
        url_value = safe_value(item, "url", "value")

        if url_value:
            urls.append({
                "name": url_name,
                "url": url_value
            })

    return urls


def extract_employments(record: dict) -> list:
    employments = []

    groups = safe_value(
        record,
        "activities-summary",
        "employments",
        "affiliation-group",
        default=[]
    )

    for group in groups:
        summaries = group.get("summaries", [])

        for summary in summaries:
            employment = summary.get("employment-summary", {})

            organization = employment.get("organization", {})
            address = organization.get("address", {})

            employments.append({
                "organization": organization.get("name", ""),
                "city": address.get("city", ""),
                "region": address.get("region", ""),
                "country": address.get("country", ""),
                "department_name": employment.get("department-name", ""),
                "role_title": employment.get("role-title", "")
            })

    return employments


def transform_orcid_to_profiles(orcid_data: dict) -> list:
    """
    Transform ORCID records into profile-like records.
    Target idea: faculty_profiles / researcher_profiles.
    """

    profiles = []

    for item in orcid_data.get("records", []):
        orcid_id = item.get("orcid_id", "")
        source_url = item.get("source_url", "")
        record = item.get("record", {})

        person = record.get("person", {})
        name = person.get("name", {}) or {}

        given_name = safe_value(name, "given-names", "value")
        family_name = safe_value(name, "family-name", "value")
        credit_name = safe_value(name, "credit-name", "value")

        full_name = credit_name or f"{given_name} {family_name}".strip()

        biography = ""
        bio_obj = person.get("biography")
        if isinstance(bio_obj, dict):
            biography = bio_obj.get("content", "")

        employments = extract_employments(record)
        main_employment = employments[0] if employments else {}

        profile = {
            "orcid_id": orcid_id,
            "full_name": full_name,
            "given_name": given_name,
            "family_name": family_name,
            "biography": biography,
            "keywords": extract_keywords(person),
            "researcher_urls": extract_researcher_urls(person),
            "organization": main_employment.get("organization", ""),
            "role_title": main_employment.get("role_title", ""),
            "department_name": main_employment.get("department_name", ""),
            "city": main_employment.get("city", ""),
            "country": main_employment.get("country", ""),
            "source": SOURCE_NAME,
            "scrape_timestamp": datetime.now().isoformat()
        }

        profiles.append(
            create_common_fields(
                source_system=SOURCE_SYSTEM,
                source_url=source_url,
                data=profile
            )
        )

    return profiles


def extract_doi(work_summary: dict) -> str:
    external_ids = safe_value(work_summary, "external-ids", "external-id", default=[])

    for external_id in external_ids:
        if external_id.get("external-id-type") == "doi":
            return external_id.get("external-id-value", "")

    return ""


def transform_orcid_to_publications(orcid_data: dict) -> list:
    """
    Transform ORCID works into research publication records.
    Target idea: research_publications.
    """

    publications = []

    for item in orcid_data.get("records", []):
        orcid_id = item.get("orcid_id", "")
        researcher_url = item.get("source_url", "")
        record = item.get("record", {})

        person = record.get("person", {})
        name = person.get("name", {}) or {}

        given_name = safe_value(name, "given-names", "value")
        family_name = safe_value(name, "family-name", "value")
        author_name = f"{given_name} {family_name}".strip()

        work_groups = safe_value(
            record,
            "activities-summary",
            "works",
            "group",
            default=[]
        )

        for group in work_groups:
            summaries = group.get("work-summary", [])

            for work in summaries:
                title = safe_value(work, "title", "title", "value", default="No title")
                doi = extract_doi(work)

                year = safe_value(
                    work,
                    "publication-date",
                    "year",
                    "value"
                )

                journal = safe_value(
                    work,
                    "journal-title",
                    "value",
                    default=""
                )

                publication_url = safe_value(work, "url", "value")
                if not publication_url and doi:
                    publication_url = f"https://doi.org/{doi}"

                publication = {
                    "orcid_id": orcid_id,
                    "author_name": author_name,
                    "title": title,
                    "doi": doi,
                    "publication_year": year,
                    "journal": journal,
                    "publication_type": work.get("type", ""),
                    "source": SOURCE_NAME,
                    "source_url": publication_url or researcher_url,
                    "scrape_timestamp": datetime.now().isoformat()
                }

                publications.append(
                    create_common_fields(
                        source_system=SOURCE_SYSTEM,
                        source_url=publication.get("source_url", researcher_url),
                        data=publication
                    )
                )

    return publications


# ==============================================================
# MAIN RUNNER
# ==============================================================

def run(limit: int = MAX_RECORDS, query: str = SEARCH_QUERY):
    """
    Extract ORCID public records and store:
    - raw data + hudi-ready data in raw-json
    - execution log in raw-logs
    """

    client = MinIOClient()
    partition = get_date_partition()
    timestamp = partition["timestamp"]

    status = 500
    records = 0
    publications_count = 0
    profiles_count = 0

    print("=" * 60)
    print("ORCID PUBLIC API INGESTION")
    print("=" * 60)
    print(f"Query: {query}")
    print(f"Max records: {limit}")
    print("Buckets:")
    print("  - raw-json")
    print("  - raw-logs")
    print("=" * 60)

    try:
        raw_data, status = extract_orcid_records(
            query=query,
            limit=limit
        )

        records = len(raw_data.get("records", []))

        faculty_profiles = transform_orcid_to_profiles(raw_data)
        research_publications = transform_orcid_to_publications(raw_data)

        profiles_count = len(faculty_profiles)
        publications_count = len(research_publications)

        complete_package = {
            "metadata": {
                "source": SOURCE_NAME,
                "source_type": "api",
                "query": query,
                "requested_limit": limit,
                "records_fetched": records,
                "profiles_count": profiles_count,
                "publications_count": publications_count,
                "extraction_timestamp": partition["iso"],
                "extraction_date": (
                    f"{partition['year']}-"
                    f"{partition['month']}-"
                    f"{partition['day']}"
                )
            },
            "raw_data": raw_data,
            "hudi_ready_data": {
                "faculty_profiles": faculty_profiles,
                "research_publications": research_publications
            }
        }

        object_path = (
            f"source={SOURCE_NAME}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"orcid_records_{timestamp}.json"
        )

        client.upload_json(
            bucket_name="raw-json",
            object_name=object_path,
            data=complete_package
        )

        print("\nORCID extraction completed")
        print(f"Records fetched: {records}")
        print(f"Profiles prepared: {profiles_count}")
        print(f"Publications prepared: {publications_count}")
        print(f"Saved to: raw-json/{object_path}")

    except requests.exceptions.Timeout:
        print("Error: timeout during ORCID API call")
        status = 408

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        status = e.response.status_code if e.response is not None else 500

    except requests.exceptions.RequestException as e:
        print(f"Network Error: {e}")
        status = 500

    except Exception as e:
        print(f"Unexpected Error: {e}")
        status = 500

    finally:
        log = {
            "source": SOURCE_NAME,
            "source_type": "api",
            "operation": "extract",
            "status": status,
            "query": query,
            "records_requested": limit,
            "records_fetched": records,
            "profiles_prepared": profiles_count,
            "publications_prepared": publications_count,
            "timestamp": partition["iso"]
        }

        log_path = (
            f"source={SOURCE_NAME}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"log_{timestamp}.json"
        )

        try:
            client.upload_json(
                bucket_name="raw-logs",
                object_name=log_path,
                data=log
            )

            print(f"Log saved to: raw-logs/{log_path}")

        except Exception as e:
            print(f"Error uploading log: {e}")

        print("=" * 60)
        print(f"Final status: {status}")
        print("=" * 60)


if __name__ == "__main__":
    run()