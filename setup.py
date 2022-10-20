import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="bigquery-glue-connector",
    version="0.0.1",

    description="(SO9084) AWS Glue Connector Ingestion for GA4 Analytics v1.0.0 - This guidance provides a reference architecture for the ingestion of Google Analytics v4 data from Google Cloud BigQuery into AWS for data analytics and collaboration.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="bmaguiraz",

    package_dir={"": "lib/glue/packages"},
    packages=setuptools.find_packages(where="packages"),

    install_requires=[
        "aws-cdk-lib>=2.43.1",
        "constructs>=10.1.105",
        "aws-cdk.aws_glue_alpha",
        "aws-cdk.aws_glue",
        "cdk-nag>=2.18.3",
        "aws-glue-alpha",
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: MIT-0",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
