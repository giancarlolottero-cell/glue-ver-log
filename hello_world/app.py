import json
import os
from requests.utils import requote_uri
import pandas as pd


def _build_summary(event):
    """Build a small request summary using pandas."""
    method = event.get("httpMethod", "UNKNOWN")
    path = requote_uri(event.get("path", "/"))
    query_params = event.get("queryStringParameters") or {}
    stage = os.environ.get("STAGE", "dev")

    df = pd.DataFrame(
        [
            {
                "method": method,
                "path": path,
                "query_param_count": len(query_params),
                "stage": stage,
            }
        ]
    )

    return df.to_dict(orient="records")[0]


def lambda_handler(event, context):
    """Handle API Gateway Lambda proxy requests."""
    try:
        summary = _build_summary(event)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(
                {
                    "message": "hello world",
                    "summary": summary,
                }
            ),
        }
    except Exception as error:
        print(f"Error in lambda_handler: {error}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(
                {
                    "message": "internal server error",
                }
            ),
        }
