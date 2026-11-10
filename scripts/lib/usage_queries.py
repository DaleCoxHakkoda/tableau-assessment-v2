"""Admin Insights (Tableau Cloud only) view-usage queries for step 02.

Tableau Server has no Admin Insights; Server usage comes from REST snapshots
(see fetch_view_usage.py).
"""

# ---------------------------------------------------------------- Admin Insights (Cloud)
def _ai_filters(min_date):
    return [
        {"field": {"fieldCaption": "Event Type"}, "filterType": "SET", "values": ["Access"]},
        {"field": {"fieldCaption": "Item Type"}, "filterType": "SET", "values": ["View"]},
        {"field": {"fieldCaption": "Event Date"}, "filterType": "QUANTITATIVE_DATE",
         "quantitativeFilterType": "MIN", "minDate": min_date},
    ]


def ai_per_view(min_date):
    return {"fields": [
        {"fieldCaption": "Item LUID", "fieldAlias": "view_luid"},
        {"fieldCaption": "Item Name", "fieldAlias": "view_name"},
        {"fieldCaption": "Workbook Name", "fieldAlias": "workbook"},
        {"fieldCaption": "Project Name", "fieldAlias": "project"},
        {"fieldCaption": "Number of Events", "function": "SUM", "fieldAlias": "accesses",
         "sortDirection": "DESC", "sortPriority": 1},
        {"fieldCaption": "Actor User Id", "function": "COUNTD", "fieldAlias": "actors"},
        {"fieldCaption": "Event Date", "function": "MIN", "fieldAlias": "first_accessed"},
        {"fieldCaption": "Event Date", "function": "MAX", "fieldAlias": "last_accessed"},
    ], "filters": _ai_filters(min_date)}


def ai_per_view_week(min_date):
    return {"fields": [
        {"fieldCaption": "Item LUID", "fieldAlias": "view_luid"},
        {"fieldCaption": "Event Date", "function": "TRUNC_WEEK", "fieldAlias": "week"},
        {"fieldCaption": "Number of Events", "function": "SUM", "fieldAlias": "accesses"},
    ], "filters": _ai_filters(min_date)}


def ai_per_view_user(min_date):
    return {"fields": [
        {"fieldCaption": "Item LUID", "fieldAlias": "view_luid"},
        {"fieldCaption": "Actor User Name", "fieldAlias": "user"},
        {"fieldCaption": "Number of Events", "function": "SUM", "fieldAlias": "accesses"},
    ], "filters": _ai_filters(min_date)}
