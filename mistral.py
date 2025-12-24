import csv
import json
import os
import time
from mistralai import Mistral

# Get artist demographics data using Mistral AI

# Configuration
INPUT_CSV = "artists_unique.csv"
OUTPUT_CSV = "artists_demographics.csv"
BATCH_SIZE = 100
MAX_ATTEMPTS = 3
PAUSE_SECONDS = 2

# Mistral API configuration
API_KEY = os.environ.get("MISTRAL_API_KEY", "VzUb04bpVuOBJHzLFrb3pdqUOgydGVhG")
MODEL = "mistral-large-latest"

# Initialize Mistral client
client = Mistral(api_key=API_KEY)

def build_prompt(artists):
    prompt_lines = [
        "You are an assistant specializing in demographics of music artists.",
        "For each artist listed below, provide their gender and age.",
        "",
        "Guidelines:",
        "- If you're uncertain about gender or age, use 'nan'.",
        "- Output ONLY in CSV format as: artist,gender,age",
        "",
        "Artists:"
    ]
    prompt_lines += artists
    return "\n".join(prompt_lines)

def parse_response(response_text):
    response_text = response_text.strip()
    if "```csv" in response_text or "```" in response_text:
        response_text = response_text.split("```csv")[-1].split("```", 1)[0].strip()
    lines = response_text.split("\n")
    reader = csv.DictReader(lines)
    return [row for row in reader]

# Load artists
with open(INPUT_CSV, newline='', encoding='utf-8') as f:
    artists = [row['artist'] for row in csv.DictReader(f)]

results = []

# Process artists in batches
for i in range(0, len(artists), BATCH_SIZE):
    batch = artists[i:i+BATCH_SIZE]
    prompt = build_prompt(batch)

    attempts = MAX_ATTEMPTS
    while attempts:
        try:
            response = client.chat.complete(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}]
            )

            content = response.choices[0].message.content
            parsed_results = parse_response(content)
            results.extend(parsed_results)
            print(f"Processed batch {i//BATCH_SIZE + 1}")
            break

        except Exception as e:
            attempts -= 1
            print(f"Error: {e}. Retrying in {PAUSE_SECONDS}s ({attempts} attempts left)")
            time.sleep(PAUSE_SECONDS)

df = pd.DataFrame(results)

# Save the DataFrame to a CSV file
df.to_csv('artists_demographics.csv', index=False)