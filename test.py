import os
from typing import List, Dict, Any, Optional, Union
from pymongo import MongoClient
from bson import json_util, ObjectId
import json
import ssl
import re
import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain.schema import SystemMessage, HumanMessage

# Load environment variables
load_dotenv()

class MongoDBQueryEngine:
    def __init__(
        self,
        mongodb_uri: str,
        database_name: str,
        google_api_key: str,
    ):
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.google_api_key = google_api_key
        self.client = MongoClient(
            mongodb_uri,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=30000,
            ssl=True,
            tlsAllowInvalidCertificates=True
        )
        self.db = self.client[database_name]
        self.collections = {}  # Cache of collection metadata
        self.current_collection = None
        self.chat_history = []

        # Initialize LLM for query generation
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash",
            google_api_key=google_api_key,
            temperature=0.2
        )
        
        # Initialize embeddings using Google's embeddings
        self.embeddings = GoogleGenerativeAIEmbeddings(
            model="models/embedding-001",
            google_api_key=google_api_key,
            task_type="retrieval_query"
        )
        
        # Load database schema
        self._load_database_schema()

    def _load_database_schema(self):
        """Load schema information about all collections in the database"""
        print(f"Connecting to MongoDB database: {self.database_name}")
        
        try:
            self.client.server_info()  # Check connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")

        # Get list of all collections
        collection_names = self.db.list_collection_names()
        print(f"Found {len(collection_names)} collections: {', '.join(collection_names)}")
        
        # Create schema information for each collection
        # for col_name in collection_names:
        #     if col_name == "system.views":
        #         continue
        #     print(f"Analyzing collection: {col_name}")
        self._analyze_collection("portcalls")
    
    def _analyze_collection(self, collection_name: str, sample_size: int = 100):
        """Analyze a collection's schema from a sample of documents"""
        collection = self.db[collection_name]
        
        # Count total documents
        total_count = collection.count_documents({})
        
        # Get a sample of documents
        sample_docs = list(collection.find().limit(sample_size))
        
        # Extract field information
        fields = {}
        for doc in sample_docs:
            self._analyze_document(doc, fields)
        
        # Convert sets to lists for JSON serialization
        for field in fields:
            fields[field]["types"] = list(fields[field]["types"])
        
        # Store collection metadata
        self.collections[collection_name] = {
            "total_documents": total_count,
            "fields": fields,
            "sample_document": json.loads(json_util.dumps(sample_docs[0] if sample_docs else {}))
        }
        
        # Detect date fields for trend analysis
        date_fields = [field for field, info in fields.items() 
                      if "datetime" in info["types"] or "date" in info["types"]]
        
        if date_fields:
            self.collections[collection_name]["date_fields"] = date_fields
    
    def _analyze_document(self, doc, fields, prefix=""):
        """Recursively analyze document structure including nested fields"""
        for key, value in doc.items():
            field_name = f"{prefix}{key}"
            
            if field_name not in fields:
                fields[field_name] = {"types": set(), "examples": []}
            
            # Handle special types
            if isinstance(value, datetime.datetime):
                value_type = "datetime"
            elif isinstance(value, ObjectId):
                value_type = "objectid"
            else:
                value_type = type(value).__name__
                
            fields[field_name]["types"].add(value_type)
            
            # Store examples for non-ID fields
            if field_name != "_id" and len(fields[field_name]["examples"]) < 3:
                fields[field_name]["examples"].append(str(value))
            
            # Recursively analyze nested documents
            if isinstance(value, dict):
                self._analyze_document(value, fields, f"{field_name}.")
            
            # Handle arrays of objects
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Sample the first item in the array
                self._analyze_document(value[0], fields, f"{field_name}[].")
    
    def set_current_collection(self, collection_name: str):
        """Set the current collection for queries"""
        if collection_name not in self.collections:
            if collection_name in self.db.list_collection_names():
                self._analyze_collection(collection_name)
            else:
                raise ValueError(f"Collection '{collection_name}' does not exist in the database")
        
        self.current_collection = collection_name
        print(f"Current collection set to: {collection_name}")
        return f"Current collection set to: {collection_name} ({self.collections[collection_name]['total_documents']} documents)"
    
    def list_collections(self):
        """List all collections in the database with document counts"""
        collection_info = []
        for name in self.collections:
            doc_count = self.collections[name]["total_documents"]
            collection_info.append(f"{name} ({doc_count} documents)")
        
        return collection_info
    
    def get_collection_schema(self, collection_name: Optional[str] = None):
        """Get schema information for a collection"""
        col_name = collection_name or self.current_collection
        
        if not col_name:
            raise ValueError("No collection specified and no current collection set")
        
        if col_name not in self.collections:
            raise ValueError(f"Collection '{col_name}' not found")
        
        schema_info = []
        for field, info in self.collections[col_name]["fields"].items():
            types_str = ", ".join(info["types"])
            examples_str = ", ".join([f"'{ex}'" for ex in info["examples"]]) if info["examples"] else "N/A"
            schema_info.append(f"Field: {field}, Types: {types_str}, Examples: {examples_str}")
        
        return schema_info

    def _fix_date_formats(self, query):
        """Fix date formats in MongoDB queries by converting string date operators to proper MongoDB operators"""
        if isinstance(query, dict):
            fixed_query = {}
            for k, v in query.items():
                # Handle date operators
                if k == "$dateFromString" and isinstance(v, dict) and "dateString" in v and "format" in v:
                    # Convert to a safer approach without format specifier
                    fixed_query[k] = {"dateString": v["dateString"]}
                elif k in ("$dateToString", "$dateToParts", "$dateFromParts"):
                    # Handle date conversion operators
                    if isinstance(v, dict) and "format" in v:
                        # Use ISO format instead of custom format
                        new_v = v.copy()
                        if "format" in new_v:
                            new_v["format"] = "%Y-%m-%dT%H:%M:%S.%LZ"  # Use ISO format
                        fixed_query[k] = new_v
                    else:
                        fixed_query[k] = self._fix_date_formats(v)
                # Recursively process nested elements
                elif isinstance(v, dict):
                    fixed_query[k] = self._fix_date_formats(v)
                elif isinstance(v, list):
                    fixed_query[k] = [self._fix_date_formats(item) if isinstance(item, (dict, list)) else item for item in v]
                else:
                    fixed_query[k] = v
            return fixed_query
            
        elif isinstance(query, list):
            return [self._fix_date_formats(item) if isinstance(item, (dict, list)) else item for item in query]
            
        else:
            return query

    def generate_mongodb_query(self, question: str):
        """Generate a MongoDB query or aggregation pipeline based on a natural language question"""
        # Prepare context with collection information
        if not self.current_collection:
            raise ValueError("No collection selected. Please select a collection first.")
        
        collection_info = self.collections[self.current_collection]
        sample_doc = collection_info["sample_document"]
        print("11111111111111111")
        print("4444444444444444444444444")
        # Format field information
        fields_info = "\n".join([
            f"- {field} (types: {', '.join(info['types'])}, examples: {', '.join(info['examples']) if info['examples'] else 'N/A'})"
            for field, info in collection_info["fields"].items()
        ])
        print("777777777777777777777777777777777")
        # Add specialized handling for time-series and trend analysis
        trend_guidance = ""
        if "date_fields" in collection_info and collection_info["date_fields"]:
            date_fields = collection_info["date_fields"]
            trend_guidance = f"""
            For time-series or trend analysis:
            - Available date fields: {', '.join(date_fields)}
            - For current month, use: {{ $gte: new Date(new Date().getFullYear(), new Date().getMonth(), 1), $lt: new Date(new Date().getFullYear(), new Date().getMonth()+1, 1) }}
            - Avoid using $dateFromString with format specifiers - use simpler date expressions like ISODate()
            - For aggregations by time period, use $group with date operators like $dayOfMonth, $month, $year
            """
        print("555555555555555555555555555555555555555555")
        # Create prompt for query generation
        prompt = f"""
        You are a MongoDB query expert. Generate a MongoDB query or aggregation pipeline to answer the following question.

        Database: {self.database_name}
        Collection: {self.current_collection}
        Total documents: {collection_info['total_documents']}

        Collection fields:
        {fields_info}

        Sample document:
        {json.dumps(sample_doc, indent=2)}
        
        {trend_guidance}

        User question: {question}

        Important guidelines:
        1. For time trends, use standard MongoDB date operators like $dayOfMonth, $month, $year in $group stages
        2. Avoid complex date format strings in $dateToString or $dateFromString
        3. For date comparisons, use ISODate() or new Date() expressions
        4. For trend analysis, use $group and $sort on time periods 
        5. Ensure all operators use proper MongoDB syntax

        Generate ONLY the MongoDB query or aggregation pipeline as a JSON object with these fields:
        - query_type: 'find', 'aggregate', 'count', or 'distinct'
        - query: The query object or pipeline array
        - explanation: Brief explanation of what the query does

        Format:
        ```json
        {{
        "query_type": "find|aggregate|count|distinct",
        "query": {{...}} or [...],
        "explanation": "..."
        }}
        ```
        """
       
        # Generate query using LLM
        messages = [
            SystemMessage(content="You are a MongoDB query expert assistant that outputs only valid JSON objects."),
            HumanMessage(content=prompt)
        ]
        print("-----------------------")
        response = self.llm.invoke(messages)
        print("---------------------------",response)
        response_text = response.content
        
        # Extract JSON from the response
        json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
        match = re.search(json_pattern, response_text)
        
        if match:
            print("uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu")
            json_str = match.group(1)
            try:
                print("ssssssssssssssssssssssssssss")
                query_data = json.loads(json_str)
                print("tttttttttttttttttttttttttttttttttttt",query_data)
                # Fix potential date format issues
                if "query" in query_data:
                    print("lllllllllllllllllllllllll")
                    query_data["query"] = self._fix_date_formats(query_data["query"])
                print("//////////////",query_data)
                return query_data
                # pipeline = {
                #     "query_type": "aggregate",
                #     "query":[
                #     {
                #         "$match": {
                #             "createdOn": {
                #                 "$gte": "2025-04-01T00:00:00Z",
                #                 "$lte": "2025-04-31T23:59:59Z"
                #             }
                #         }
                #     },
                #     {
                #         "$count": "portcall_count"
                #     }
                # ]}
                return pipeline
            except json.JSONDecodeError as e:
                raise ValueError(f"Generated invalid JSON: {e}\nJSON string: {json_str}")
        else:
            print("hhhhhhhhhhhhhhhhhhhhhh")
            # Try to parse the whole response as JSON if no code block found
            try:
                query_data = json.loads(response_text)
                if "query" in query_data:
                    query_data["query"] = self._fix_date_formats(query_data["query"])
                print("sssssssssssssssssssssss",query_data)    
                return query_data
            except:
                raise ValueError(f"Could not extract valid JSON from response: {response_text}")
    def _handle_date_in_query(self, query):
        """Convert date strings to date objects in queries"""
        if isinstance(query, dict):
            for key, value in list(query.items()):
                if isinstance(value, dict):
                    query[key] = self._handle_date_in_query(value)
                elif isinstance(value, list):
                    query[key] = [self._handle_date_in_query(item) if isinstance(item, (dict, list)) else item for item in value]
                elif key == "$date" and isinstance(value, str):
                    # Handle {"$date": "2023-01-01"} pattern
                    try:
                        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
                    except ValueError:
                        pass
                
                # Handle special MongoDB date expressions that might be in string format
                if isinstance(value, str):
                    # Handle "new Date(...)" pattern
                    if value.startswith("new Date(") and value.endswith(")"):
                        try:
                            # Parse arguments of new Date()
                            args_str = value[9:-1]  # Extract content between new Date( and )
                            if args_str:
                                if "," in args_str:  # new Date(year, month, day)
                                    args = [int(arg.strip()) for arg in args_str.split(",")]
                                    if len(args) >= 3:
                                        # MongoDB months are 0-indexed
                                        return datetime.datetime(args[0], args[1] + 1, args[2])
                                else:  # new Date("2023-01-01")
                                    # Remove quotes if present
                                    date_str = args_str
                                    if date_str.startswith('"') and date_str.endswith('"'):
                                        date_str = date_str[1:-1]
                                    if date_str.startswith("'") and date_str.endswith("'"):
                                        date_str = date_str[1:-1]
                                    return datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                            else:  # new Date() - current date
                                return datetime.datetime.now()
                        except Exception:
                            pass
                        
                    # Handle "ISODate(...)" pattern
                    elif value.startswith("ISODate(") and value.endswith(")"):
                        try:
                            date_str = value[8:-1]  # Extract content between ISODate( and )
                            # Remove quotes if present
                            if date_str.startswith('"') and date_str.endswith('"'):
                                date_str = date_str[1:-1]
                            if date_str.startswith("'") and date_str.endswith("'"):
                                date_str = date_str[1:-1]
                            return datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        except Exception:
                            pass
            
            # Special handling for date expressions that should be converted to MongoDB operators
            if "$currentDate" in query:
                query["$currentDate"] = True
            
        elif isinstance(query, list):
            return [self._handle_date_in_query(item) if isinstance(item, (dict, list)) else item for item in query]
        
        return query

    def execute_query(self, query_data: Dict[str, Any]):
        """Execute a MongoDB query based on the query data"""
        if not self.current_collection:
            raise ValueError("No collection selected")
        
        collection = self.db[self.current_collection]
        query_type = query_data.get("query_type", "").lower()
        query = query_data.get("query", {})
        explanation = query_data.get("explanation", "")
        
        # Pre-process and fix date-related issues in the query
        query = self._handle_date_in_query(query)
        
        print(f"Executing {query_type} query: {json.dumps(query, default=str)}")
        print(f"Explanation: {explanation}")
        
        try:
            if query_type == "find":
                cursor = collection.find(query)
                results = list(cursor.limit(50))  # Limit to first 50 results for safety
                return {
                    "count": len(results),
                    "results": json.loads(json_util.dumps(results)),
                    "explanation": explanation
                }
            
            elif query_type == "aggregate":
                # Add a $limit stage if not present for safety
                has_limit = any(stage.get("$limit") is not None for stage in query if isinstance(stage, dict))
                if not has_limit:
                    query.append({"$limit": 50})
                
                cursor = collection.aggregate(query)
                results = list(cursor)
                return {
                    "count": len(results),
                    "results": json.loads(json_util.dumps(results)),
                    "explanation": explanation
                }
            
            elif query_type == "count":
                count = collection.count_documents(query)
                return {
                    "count": count,
                    "explanation": explanation
                }
            
            elif query_type == "distinct":
                field = query.get("field")
                filter_query = query.get("filter", {})
                if not field:
                    raise ValueError("'field' is required for distinct queries")
                
                values = collection.distinct(field, filter_query)
                return {
                    "count": len(values),
                    "values": values,
                    "explanation": explanation
                }
            
            else:
                raise ValueError(f"Unsupported query type: {query_type}")
                
        except Exception as e:
            raise RuntimeError(f"Error executing query: {str(e)}")
    
    def get_time_range_query(self, time_range: str):
        """Generate a date range query based on a time range description"""
        today = datetime.datetime.now()
        
        if time_range == "today":
            start_date = datetime.datetime(today.year, today.month, today.day)
            end_date = start_date + datetime.timedelta(days=1)
        elif time_range == "yesterday":
            start_date = datetime.datetime(today.year, today.month, today.day) - datetime.timedelta(days=1)
            end_date = start_date + datetime.timedelta(days=1)
        elif time_range == "this_week":
            # Start of current week (Monday)
            start_date = today - datetime.timedelta(days=today.weekday())
            start_date = datetime.datetime(start_date.year, start_date.month, start_date.day)
            end_date = start_date + datetime.timedelta(days=7)
        elif time_range == "last_week":
            # Start of previous week
            start_date = today - datetime.timedelta(days=today.weekday() + 7)
            start_date = datetime.datetime(start_date.year, start_date.month, start_date.day)
            end_date = start_date + datetime.timedelta(days=7)
        elif time_range == "this_month":
            start_date = datetime.datetime(today.year, today.month, 1)
            if today.month == 12:
                end_date = datetime.datetime(today.year + 1, 1, 1)
            else:
                end_date = datetime.datetime(today.year, today.month + 1, 1)
        elif time_range == "last_month":
            if today.month == 1:
                start_date = datetime.datetime(today.year - 1, 12, 1)
                end_date = datetime.datetime(today.year, 1, 1)
            else:
                start_date = datetime.datetime(today.year, today.month - 1, 1)
                end_date = datetime.datetime(today.year, today.month, 1)
        elif time_range == "last_6_months":
            six_months_ago = today - relativedelta(months=6)
            start_date = datetime.datetime(six_months_ago.year, six_months_ago.month, 1)
            end_date = datetime.datetime(today.year, today.month, 1) + relativedelta(months=1)
        elif time_range == "this_year":
            start_date = datetime.datetime(today.year, 1, 1)
            end_date = datetime.datetime(today.year + 1, 1, 1)
        elif time_range == "last_year":
            start_date = datetime.datetime(today.year - 1, 1, 1)
            end_date = datetime.datetime(today.year, 1, 1)
        else:
            raise ValueError(f"Unsupported time range: {time_range}")
        
        return {"$gte": start_date, "$lt": end_date}
    
    def get_collection_stats(self, collection_name: Optional[str] = None):
        """Get detailed statistics about a collection"""
        col_name = collection_name or self.current_collection
        
        if not col_name:
            raise ValueError("No collection specified and no current collection set")
        
        collection = self.db[col_name]
        
        # Get basic collection stats
        stats = {
            "name": col_name,
            "document_count": collection.count_documents({}),
            "field_stats": {}
        }
        
        # For each field, get distribution of values if cardinality is reasonable
        if col_name in self.collections:
            for field_name in self.collections[col_name]["fields"]:
                if field_name == "_id":
                    continue
                    
                # Get unique value count
                distinct_count = len(collection.distinct(field_name))
                
                if distinct_count <= 15:  # Only get distribution for low-cardinality fields
                    pipeline = [
                        {"$group": {"_id": f"${field_name}", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 10}
                    ]
                    distribution = list(collection.aggregate(pipeline))
                    
                    stats["field_stats"][field_name] = {
                        "distinct_values": distinct_count,
                        "distribution": [{"value": item["_id"], "count": item["count"]} for item in distribution]
                    }
                else:
                    stats["field_stats"][field_name] = {
                        "distinct_values": distinct_count
                    }
                    
            # Add time series stats if date fields exist
            if "date_fields" in self.collections[col_name] and self.collections[col_name]["date_fields"]:
                primary_date_field = self.collections[col_name]["date_fields"][0]
                stats["time_stats"] = {}
                
                # Get date range
                pipeline = [
                    {"$group": {
                        "_id": None,
                        "min_date": {"$min": f"${primary_date_field}"},
                        "max_date": {"$max": f"${primary_date_field}"}
                    }}
                ]
                date_range = list(collection.aggregate(pipeline))
                if date_range:
                    stats["time_stats"]["date_range"] = {
                        "min": date_range[0]["min_date"],
                        "max": date_range[0]["max_date"]
                    }
                
                # Get counts by month for recent data
                pipeline = [
                    {"$match": {
                        primary_date_field: {
                            "$gte": datetime.datetime.now() - relativedelta(months=6)
                        }
                    }},
                    {"$group": {
                        "_id": {
                            "year": {"$year": f"${primary_date_field}"},
                            "month": {"$month": f"${primary_date_field}"}
                        },
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"_id.year": 1, "_id.month": 1}}
                ]
                monthly_counts = list(collection.aggregate(pipeline))
                if monthly_counts:
                    stats["time_stats"]["monthly_counts"] = [
                        {
                            "year": item["_id"]["year"],
                            "month": item["_id"]["month"],
                            "count": item["count"]
                        }
                        for item in monthly_counts
                    ]
        
        return stats

    def process_analytical_question(self, question: str):
        """Process analytical questions that require specialized handling"""
        if not self.current_collection:
            return "No collection selected. Please select a collection first."
            
        collection_info = self.collections[self.current_collection]
        
        # Check if this is a trend analysis question
        trend_keywords = ["trend", "over time", "monthly", "weekly", "daily", "comparison", "historical", "history", "pattern"]
        is_trend_question = any(keyword in question.lower() for keyword in trend_keywords)
        
        if is_trend_question and "date_fields" in collection_info and collection_info["date_fields"]:
            primary_date_field = collection_info["date_fields"][0]
            
            # Extract time range information from the question
            current_month_keywords = ["current month", "this month"]
            last_month_keywords = ["last month", "previous month"]
            last_6_months_keywords = ["last 6 months", "past 6 months", "previous 6 months"]
            
            time_periods = []
            
            if any(keyword in question.lower() for keyword in current_month_keywords):
                time_periods.append("this_month")
            if any(keyword in question.lower() for keyword in last_month_keywords):
                time_periods.append("last_month")
            if any(keyword in question.lower() for keyword in last_6_months_keywords):
                time_periods.append("last_6_months")
                
            # Default to current month + previous 6 months if no specific period mentioned
            if not time_periods:
                time_periods = ["this_month", "last_6_months"]
                
            # Determine appropriate aggregation level (daily, weekly, monthly)
            if "daily" in question.lower() or "day" in question.lower():
                group_by = "day"
            elif "weekly" in question.lower() or "week" in question.lower():
                group_by = "week"
            else:
                group_by = "month"  # default to monthly
                
            return self._generate_trend_analysis(primary_date_field, time_periods, group_by)
            
        # For other analytical questions, use the standard query generator
        return None
    
    def _generate_trend_analysis(self, date_field: str, time_periods: List[str], group_by: str = "month"):
        """Generate a trend analysis for specified time periods and grouping"""
        # Get appropriate MongoDB date operator for grouping
        group_id = {}
        if group_by == "day":
            group_id = {
                "year": {"$year": f"${date_field}"},
                "month": {"$month": f"${date_field}"},
                "day": {"$dayOfMonth": f"${date_field}"}
            }
            sort_order = ["year", "month", "day"]
        elif group_by == "week":
            group_id = {
                "year": {"$year": f"${date_field}"},
                "week": {"$week": f"${date_field}"}
            }
            sort_order = ["year", "week"]
        else:  # month
            group_id = {
                "year": {"$year": f"${date_field}"},
                "month": {"$month": f"${date_field}"}
            }
            sort_order = ["year", "month"]
            
        # Build date range query for all time periods
        date_queries = {}
        for period in time_periods:
            date_queries[period] = self.get_time_range_query(period)
            
        # Build aggregation pipeline for each time period
        results = {}
        collection = self.db[self.current_collection]
        
        for period, date_query in date_queries.items():
            pipeline = [
                {"$match": {date_field: date_query}},
                {"$group": {
                    "_id": group_id,
                    "count": {"$sum": 1}
                }},
                {"$sort": {f"_id.{field}": 1 for field in sort_order}}
            ]
            
            try:
                cursor = collection.aggregate(pipeline)
                results[period] = list(cursor)
            except Exception as e:
                results[period] = f"Error: {str(e)}"
                
        # Format results for display
        response = f"Trend analysis of {self.current_collection} by {group_by}\n\n"
        
        for period, data in results.items():
            if isinstance(data, str):  # Error occurred
                response += f"{period.replace('_', ' ').title()}: {data}\n\n"
                continue
                
            response += f"{period.replace('_', ' ').title()} ({len(data)} data points):\n"
            
            if not data:
                response += "  No data found for this period\n\n"
                continue
                
            # Format the results based on grouping
            for item in data:
                if group_by == "day":
                    date_str = f"{item['_id']['year']}-{item['_id']['month']:02d}-{item['_id']['day']:02d}"
                elif group_by == "week":
                    date_str = f"{item['_id']['year']} Week {item['_id']['week']}"
                else:  # month
                    date_str = f"{item['_id']['year']}-{item['_id']['month']:02d}"
                    
                response += f"  {date_str}: {item['count']} portcalls\n"
                
            response += "\n"
            
        return response

    def ask(self, question: str):
        """Process a natural language question about the data"""
        # Check if this is a command to switch collections or get info
        if question.lower().startswith("use collection "):
            collection_name = question[len("use collection "):].strip()
            return self.set_current_collection(collection_name)
        
        elif question.lower() == "list collections":
            collections = self.list_collections()
            return "Available collections:\n" + "\n".join(collections)
        
        elif question.lower() == "show schema" or question.lower() == "describe collection":
            if not self.current_collection:
                return "No collection selected. Use 'use collection [name]' first."
            schema = self.get_collection_schema()
            return f"Schema for {self.current_collection}:\n" + "\n".join(schema)
            
        elif question.lower() == "collection stats" or question.lower() == "stats":
            if not self.current_collection:
                return "No collection selected. Use 'use collection [name]' first."
            stats = self.get_collection_stats()
            return self._format_stats(stats)
        
        # If no collection is selected yet, ask the user to select one
        if not self.current_collection:
            return "Please select a collection first using 'use collection [name]'"
        
        try:
            print("=====================",question)
            # First, check if this is a trend analysis or specialized analytical query
            analytical_result = self.process_analytical_question(question)
            print("8888888888888888888888888",analytical_result)
            if analytical_result:
                return analytical_result
                
            # Otherwise, generate and execute the appropriate MongoDB query
            query_data = self.generate_mongodb_query(question)
            print("0000000000000000000000",query_data)
            result = self.execute_query(query_data)
            
            print("5555555555555555555555",result)
            # Format the result for display
            response = self._format_result(result, question)
            print("33333333333333333333333333",response)
            return response
            
        except Exception as e:
            return f"Error: {str(e)}"
    
    def _format_result(self, result: Dict[str, Any], question: str) -> str:
        """Format the query result into a readable response"""
        # Add the explanation from the query
        response = f"{result.get('explanation', 'Query executed successfully.')}\n\n"
        
        # Add count information
        if 'count' in result:
            response += f"Found {result['count']} results.\n\n"
        
        # For distinct queries, list the unique values
        if 'values' in result:
            values = result['values']
            if len(values) <= 20:  # Only show all values if there aren't too many
                values_str = ', '.join(str(v) for v in values)
                response += f"Distinct values: {values_str}\n\n"
            else:
                sample_values = [str(v) for v in values[:10]]
                response += f"First 10 distinct values: {', '.join(sample_values)}... (plus {len(values)-10} more)\n\n"
        
        # For queries returning documents, format them nicely
        if 'results' in result and result['results']:
            # If there are many results, just show a few
            results = result['results']
            if len(results) > 5:
                response += f"Showing first 5 of {len(results)} results:\n\n"
                results = results[:5]
            
            # Format each result
            for i, doc in enumerate(results):
                response += f"Result {i+1}:\n"
                response += json.dumps(doc, indent=2) + "\n\n"
                
        # If no results found
        elif 'results' in result and not result['results']:
            response += "No matching documents found.\n"
            
        return response
    
    def _format_stats(self, stats: Dict[str, Any]) -> str:
        """Format collection statistics into a readable response"""
        response = f"Statistics for collection: {stats['name']}\n"
        response += f"Total documents: {stats['document_count']}\n\n"
        
        response += "Field statistics:\n"
        for field, field_stats in stats['field_stats'].items():
            response += f"\nField: {field}\n"
            response += f"- Distinct values: {field_stats['distinct_values']}\n"
            
            if 'distribution' in field_stats:
                response += "- Value distribution:\n"
                for item in field_stats['distribution']:
                    value = item['value'] if item['value'] is not None else 'null'
                    response += f"  * {value}: {item['count']} documents\n"
        
        # Add time series statistics if available
        if 'time_stats' in stats:
            response += "\nTime-Series Analysis:\n"
            
            if 'date_range' in stats['time_stats']:
                min_date = stats['time_stats']['date_range']['min']
                max_date = stats['time_stats']['date_range']['max']
                response += f"- Date range: {min_date} to {max_date}\n"
            
            if 'monthly_counts' in stats['time_stats']:
                response += "- Monthly document counts:\n"
                for item in stats['time_stats']['monthly_counts']:
                    response += f"  * {item['year']}-{item['month']:02d}: {item['count']} documents\n"
        
        return response


def main():
    mongodb_uri = os.getenv("MONGO_URL")
    database_name = os.getenv("MONGO_DB")
    google_api_key = os.getenv("GOOGLE_API_KEY")
    print(google_api_key)
    if not mongodb_uri:
        mongodb_uri = input("Enter MongoDB URI: ")
    
    if not database_name:
        database_name = input("Enter database name: ")
        
    if not google_api_key:
        google_api_key = input("Enter your Google API key: ")

    try:
        query_engine = MongoDBQueryEngine(
            mongodb_uri=mongodb_uri,
            database_name=database_name,
            google_api_key=google_api_key
        )

        print("\nMongoDB Query Engine initialized successfully!")
        print("Available commands:")
        print("- list collections: Show all collections in the database")
        print("- use collection [name]: Select a collection to query")
        print("- show schema: Show the schema for the current collection")
        print("- stats: Show statistics for the current collection")
        print("- [any question]: Ask a question about the current collection")
        print("- exit: Quit the program")

        while True:
            question = input("\nEnter a command or question: ")
            if question.lower() == 'exit':
                break

            try:
                response = query_engine.ask(question)
                print(f"\n{response}")
            except Exception as e:
                print(f"Error: {e}")

    except Exception as e:
        print(f"Failed to initialize MongoDB Query Engine: {e}")


if __name__ == "__main__":
    main()