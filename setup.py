from setuptools import setup, find_packages

# Read requirements from requirements.txt
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name="jr_zoho_crm",
    version="0.1.0",
    packages=find_packages(exclude=['tests*']),  # Explicitly exclude tests
    install_requires=required,  # Use requirements from file
    description="A Zoho CRM integration project",
    long_description=open("README.md", encoding="utf-8").read(),  # Add encoding
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",  # Added development status
        "Intended Audience :: Developers",
    ],
    python_requires="==3.12",  # Stick to Python 3.12 only
)
