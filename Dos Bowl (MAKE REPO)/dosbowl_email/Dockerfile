# Use the official AWS Lambda Python 3.12 image
FROM public.ecr.aws/lambda/python:3.12

# Copy your app.py file into the container
COPY app.py ${LAMBDA_TASK_ROOT}

# If you have dependencies, copy requirements.txt and install them
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Command for AWS Lambda to execute the function
CMD ["app.lambda_handler"]