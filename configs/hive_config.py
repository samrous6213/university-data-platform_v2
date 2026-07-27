# configs/hive_config.py
HIVE_CONFIG = {
    'metastore_uris': 'thrift://localhost:9083',
    'warehouse_dir': '/user/hive/warehouse'
}

TABLE_SCHEMAS = {
    'faculty_profiles': {
        'fields': [
            ('faculty_id', 'string'),
            ('name', 'string'),
            ('title', 'string'),
            ('department', 'string'),
            ('email', 'string'),
            ('research_interests', 'array<string>'),
            ('publications_count', 'int'),
            ('source_system', 'string'),
            ('source_url', 'string'),
            ('updated_at', 'timestamp'),
            ('year', 'int'),
            ('month', 'int'),
            ('day', 'int')
        ]
    },
    'course_catalog': {
        'fields': [
            ('course_id', 'string'),
            ('title', 'string'),
            ('description', 'string'),
            ('department', 'string'),
            ('level', 'string'),
            ('instructors', 'array<string>'),
            ('topics', 'array<string>'),
            ('source_system', 'string'),
            ('source_url', 'string'),
            ('updated_at', 'timestamp'),
            ('year', 'int'),
            ('month', 'int'),
            ('day', 'int')
        ]
    },
    'research_publications': {
        'fields': [
            ('publication_id', 'string'),
            ('title', 'string'),
            ('authors', 'array<string>'),
            ('abstract', 'string'),
            ('journal', 'string'),
            ('publication_year', 'int'),
            ('doi', 'string'),
            ('citations_count', 'int'),
            ('source_system', 'string'),
            ('source_url', 'string'),
            ('updated_at', 'timestamp'),
            ('year', 'int'),
            ('month', 'int'),
            ('day', 'int')
        ]
    },
    'university_news': {
        'fields': [
            ('news_id', 'string'),
            ('title', 'string'),
            ('content', 'string'),
            ('summary', 'string'),
            ('category', 'string'),
            ('published_date', 'timestamp'),
            ('author', 'string'),
            ('source_system', 'string'),
            ('source_url', 'string'),
            ('updated_at', 'timestamp'),
            ('year', 'int'),
            ('month', 'int'),
            ('day', 'int')
        ]
    }
}