# Step 1: Use an official Python runtime as a parent image
FROM python:3.10-slim

# Step 2: Set the working directory in the container
WORKDIR /app

# Step 3: Copy the current directory contents into the container
COPY . .

# Step 4: Install any needed dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Define environment variables
ENV PATH_TO_DATA="https://drive.google.com/uc?id=1HDPOyxM6cs1SDx4boqKGrRVQam1VEPfy"

# Step 6: Specify the command to run the main script
CMD ["python", "main.py"]
