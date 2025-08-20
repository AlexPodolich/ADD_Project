from setuptools import setup, find_packages

setup(
    name="add_project",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pika",
        "psycopg2-binary",
        "python-dotenv",
        "pandas",
        "numpy",
        "scikit-learn",
        "flask",
        "flask-cors",
    ],
    python_requires=">=3.9",
) 