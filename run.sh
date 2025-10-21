#!/bin/zsh
export JAVA_HOME=$(/usr/libexec/java_home)
export PATH=$JAVA_HOME/bin:$PATH

# Set Java security options
export _JAVA_OPTIONS="-Djavax.security.auth.useSubjectCredsOnly=false --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED --add-opens=java.base/javax.security.auth=ALL-UNNAMED"

# Run the Spark processor using the virtual environment Python
/Users/atulsharma/Desktop/learning/spark-processor/.venv/bin/python main.py