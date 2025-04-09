import os
from typing import List, Dict, Any
from pymongo import MongoClient
from dotenv import load_dotenv
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.chains import ConversationalRetrievalChain
from langchain.docstore.document import Document

# Load environment variables
load_dotenv()

class MongoDBQA:
    def __init__(
        self,
        mongodb_uri: str,
        database_name: str,
        collection_name: str,
        google_api_key: str,
        query_filter: Dict[str, Any] = None,
        field_list: List[str] = None
    ):
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.google_api_key = google_api_key
        self.query_filter = query_filter or {}
        self.field_list = field_list

        self.documents = None
        self.vectorstore = None
        self.qa_chain = None
        self.chat_history = []

        self._load_documents()

    def _load_documents(self):
        print(f"Connecting to MongoDB collection: {self.collection_name}")

        # Connect to MongoDB manually
        client = MongoClient(self.mongodb_uri, serverSelectionTimeoutMS=20000)
        db = client[self.database_name]
        collection = db[self.collection_name]

        try:
            client.server_info()  # Check connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")

        projection = {field: 1 for field in self.field_list} if self.field_list else None
        cursor = collection.find(self.query_filter, projection)
        docs = list(cursor)

        print(f"Loaded {len(docs)} documents")
        if not docs:
            raise ValueError("No documents found in the collection")

        documents = [Document(page_content=str(doc), metadata={"source": self.collection_name}) for doc in docs]

        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
        self.documents = text_splitter.split_documents(documents)
        print(f"Split into {len(self.documents)} chunks")

        embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-mpnet-base-v2",
            model_kwargs={'device': 'cpu'}
        )

        self.vectorstore = Chroma.from_documents(
            documents=self.documents,
            embedding=embeddings
        )

        llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-pro",
            google_api_key=self.google_api_key,
            temperature=0.2
        )

        self.qa_chain = ConversationalRetrievalChain.from_llm(
            llm=llm,
            retriever=self.vectorstore.as_retriever(search_type="mmr", search_kwargs={"k": 5}),
            return_source_documents=True
        )

    def ask(self, question: str) -> str:
        if not self.qa_chain:
            raise ValueError("QA chain not initialized.")

        response = self.qa_chain({"question": question, "chat_history": self.chat_history})
        self.chat_history.append((question, response["answer"]))
        return response["answer"]

    def reset_chat_history(self):
        self.chat_history = []


def main():
    mongodb_uri = os.getenv("MONGO_URL")
    database_name = os.getenv("MONGO_DB")
    google_api_key = os.getenv("GOOGLE_API_KEY")

    if not google_api_key:
        google_api_key = input("Enter your Google API key: ")

    collection_name = input("Enter collection name: ")
    select_fields = input("Do you want to select specific fields? (y/n): ").lower() == 'y'
    field_list = None
    if select_fields:
        fields = input("Enter field names separated by commas: ")
        field_list = [field.strip() for field in fields.split(",")]

    try:
        qa_system = MongoDBQA(
            mongodb_uri=mongodb_uri,
            database_name=database_name,
            collection_name=collection_name,
            google_api_key=google_api_key,
            field_list=field_list
        )

        print("\nMongoDB QA system initialized successfully!")
        print("Type 'exit' to quit or 'reset' to clear chat history.")

        while True:
            question = input("\nAsk a question about your data: ")
            if question.lower() == 'exit':
                break
            elif question.lower() == 'reset':
                qa_system.reset_chat_history()
                print("Chat history reset.")
                continue

            try:
                answer = qa_system.ask(question)
                print(f"\nAnswer: {answer}")
            except Exception as e:
                print(f"Error: {e}")

    except Exception as e:
        print(f"Failed to initialize MongoDB QA system: {e}")


if __name__ == "__main__":
    main()
