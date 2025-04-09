import os
from typing import List, Dict, Any, Optional, Union
from pymongo import MongoClient
from bson import json_util
import json
import ssl
import re
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
        # mongodb_uri = mongodb_uri + "&ssl=true&ssl_cert_reqs=CERT_NONE"
        self.client =   MongoClient(
            mongodb_uri,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=30000,
            ssl=True,
            tlsAllowInvalidCertificates=True  # More modern way to disable cert verification
        )
        self.db = self.client[database_name]
        self.collections = {}  # Cache of collection metadata
        self.current_collection = None
        self.chat_history = []

        # Initialize LLM for query generation
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-pro",
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
        #     if col_name=="system.views":
        #         continue
        #     print("---------------------",col_name)
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
            for field, value in doc.items():
                if field not in fields:
                    fields[field] = {"types": set(), "examples": []}
                
                value_type = type(value).__name__
                fields[field]["types"].add(value_type)
                
                # Store a few examples of values (but not for _id)
                if field != "_id" and len(fields[field]["examples"]) < 3:
                    fields[field]["examples"].append(str(value))
        
        # Convert sets to lists for JSON serialization
        for field in fields:
            fields[field]["types"] = list(fields[field]["types"])
        
        # Store collection metadata
        self.collections[collection_name] = {
            "total_documents": total_count,
            "fields": fields,
            "sample_document": json.loads(json_util.dumps(sample_docs[0] if sample_docs else {}))
        }
    
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

    def generate_mongodb_query(self, question: str):
        """Generate a MongoDB query or aggregation pipeline based on a natural language question"""
        # Prepare context with collection information
        if not self.current_collection:
            raise ValueError("No collection selected. Please select a collection first.")
        
        collection_info = self.collections[self.current_collection]
        sample_doc = collection_info["sample_document"]
        
        # Format field information
        fields_info = "\n".join([
            f"- {field} (types: {', '.join(info['types'])}, examples: {', '.join(info['examples']) if info['examples'] else 'N/A'})"
            for field, info in collection_info["fields"].items()
        ])
        
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

        User question: {question}

        Generate ONLY the MongoDB query or aggregation pipeline as a JSON object with these fields:
        - query_type: 'find', 'aggregate', 'count', or 'distinct'
        - query: The query object or pipeline array
        - explanation: Brief explanation of what the query does

        Format:
        ```json
        {
          "query_type": "find|aggregate|count|distinct",
          "query": {...} or [...],
          "explanation": "..."
        }
        ```
        """
        
        # Generate query using LLM
        messages = [
            SystemMessage(content="You are a MongoDB query expert assistant that outputs only valid JSON objects."),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        response_text = response.content
        
        # Extract JSON from the response
        json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
        match = re.search(json_pattern, response_text)
        
        if match:
            json_str = match.group(1)
            try:
                query_data = json.loads(json_str)
                return query_data
            except json.JSONDecodeError as e:
                raise ValueError(f"Generated invalid JSON: {e}\nJSON string: {json_str}")
        else:
            # Try to parse the whole response as JSON if no code block found
            try:
                return json.loads(response_text)
            except:
                raise ValueError(f"Could not extract valid JSON from response: {response_text}")

    def execute_query(self, query_data: Dict[str, Any]):
        """Execute a MongoDB query based on the query data"""
        if not self.current_collection:
            raise ValueError("No collection selected")
        
        collection = self.db[self.current_collection]
        query_type = query_data.get("query_type", "").lower()
        query = query_data.get("query", {})
        explanation = query_data.get("explanation", "")
        
        print(f"Executing {query_type} query: {json.dumps(query)}")
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
            raise RuntimeError(f"Error executing query: {e}")
    
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
        
        return stats

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
            # Generate and execute the appropriate MongoDB query
            query_data = self.generate_mongodb_query(question)
            result = self.execute_query(query_data)
            
            # Format the result for display
            response = self._format_result(result, question)
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
        
        return response


def main():
    mongodb_uri = os.getenv("MONGO_URL")
    database_name = os.getenv("MONGO_DB")
    google_api_key = os.getenv("GOOGLE_API_KEY")
    
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