import re 
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from rapidfuzz import fuzz, process
from pep_and_sanctions.models import rica_watchlist_matches
import os
from reportsheet import gen_report as report
from RICA_spf.models import ricaspf


def preprocess_name(name, suffixes=None):
	suffixes = suffixes or []
	suffix_pattern = r'\b(' + '|'.join(re.escape(s) for s in suffixes) + r')\b' if suffixes else ''
	if suffix_pattern:
		name = re.sub(suffix_pattern, '', str(name), flags=re.IGNORECASE)
	name = re.sub(r'[^a-zA-Z0-9 ]', '', str(name))
	return name.lower().strip()

def get_emails(data=None):
	email_list=[]
	# if data.ricaEmailReceiver:
	#     email_list = [email["ricaEmailReciever"] for email in json.loads(data.ricaEmailReceiver)]

	# if data.ricaRespondent and str(data.ricaRespondentFlag).lower()=='yes':
	#     email_list.append(data.ricaRespondent.replace(" ",""))
	# if data.ricaInvestigator and str(data.ricaInvestigatorFlag).lower()=='yes':
	#     email_list.append(data.ricaInvestigator.replace(" ",""))
	# if data.ricaOwner and str(data.ricaOwnerFlag).lower()=='yes':
	#     email_list.append(data.ricaOwner.replace(" ",""))
	# if data.ricaNextOwner and str(data.ricaNextOwnerFlag).lower()=='yes':
	#     email_list.append(data.ricaNextOwner.replace(" ",""))
	email_list.append("adeleke467@gmail.com")
	email_list.append("adeniyifajemisin@gmail.com")
	# email_list.append("info@adroitsolutionsltd.com")
	# email_list.append("princetoka@hotmail.com")
	return email_list

def send_response_mail(send_to, options={}, attachment_paths=None):
	
	try:
		spf = ricaspf.objects.get(ricaSpfId__iexact=f'en-SYSTEM')
	except Exception as e:
		spf = None

	template_vars = {
		'current_date': datetime.now().strftime('%B %d, %Y'),
		'current_date_short': datetime.now().strftime('%Y-%m-%d'),
		'current_datetime': datetime.now().strftime('%B %d, %Y at %I:%M %p')
	}
	html_out = report.gen_template(
		'sanctio_bank_search.html',
		{
			**template_vars,
			'total_customers': options.get('total_customers', 0),
			'total_matches': options.get('total_matches', 0),
			'affected_customers': options.get('affected_customers', 0),
			'source_summary': options.get('source_summary', []),
		}
	)
	report.send_html_mail(
		'Bank Customer Sanctions Search Results',
		html_out,
		send_to,
		[],
		spf,
		attachment=attachment_paths or [],
		options=options
	)

def process_customer_match(cust_idx, cust_row, scores, threshold, temp_names_clean, clean_to_source):
    local_matches = []
    try:
        for idx, score_val in enumerate(scores[cust_idx]):
            if score_val >= threshold:
                cleaned_match = temp_names_clean[idx]
                rica_source = clean_to_source.get(cleaned_match, '')
                local_matches.append({
                    'Customer Name': cust_row['Customer Name'],
                    'Watchlist Match': cleaned_match,
                    'match_score': score_val,
                    'ricaSource': rica_source,
                    'account_number': cust_row.get('account_number', ''),
                    'branch_code': cust_row.get('branch_code', ''),
                    'opened_by': cust_row.get('opened_by', '')
                })
    except Exception as e:
        print(f"Error matching customer index {cust_idx}: {e}")
    return local_matches

def fuzzy_match_customers(customers, threshold=80,  watchlist_data=None):
	try:
		print("Loading customer records...", len(customers))
		try:
			temp_records = watchlist_data or []
		except Exception as e:
			print(f"Error loading watchlist records: {e}")
			return []

		clean_to_source = {}
		temp_names_clean = []
		missing_source_count = 0
		for rec in temp_records:
			try:
				cleaned = preprocess_name(rec['ricaFullName'])
				temp_names_clean.append(cleaned)
				source_val = rec.get('ricaSource', '')
				if not source_val:
					missing_source_count += 1
				clean_to_source[cleaned] = source_val
			except Exception as e:
				print(f"Error processing watchlist record: {e}")

		print(f"Loaded {len(temp_names_clean)} names from rica_temp_watchlist.")
		print(f"Diagnostics: {missing_source_count} out of {len(temp_names_clean)} watchlist records have empty ricaSource.")

		# --- Convert customers to DataFrame and preprocess names ---
		try:
			customers_df = pd.DataFrame(customers)
			if 'first_name' in customers_df.columns and 'last_name' in customers_df.columns:
				customers_df['Customer Name'] = (customers_df['first_name'].fillna('') + ' ' + customers_df['last_name'].fillna('')).str.strip()
			else:
				# fallback: use a column that exists, or the first column as name
				if 'customer_name' in customers_df.columns:
					customers_df['Customer Name'] = customers_df['customer_name'].astype(str).str.strip()
				else:
					customers_df['Customer Name'] = customers_df.iloc[:,0].astype(str).str.strip()
			print(f"Loaded {len(customers_df)} customer records from rica_temp_watchlist.")
			customers_df['name_clean'] = customers_df['Customer Name'].apply(preprocess_name)
		except Exception as e:
			print(f"Error processing customer data: {e}")
			return []

		# --- Fuzzy matching ---
		try:
			THRESHOLD = threshold
			print(f"Fuzzy matching threshold: {THRESHOLD}")
			scores = process.cdist(
				customers_df['name_clean'],
				temp_names_clean,
				scorer=fuzz.token_set_ratio,
				workers=-1  # Enables multithreaded matching
			)
			print("Fuzzy matching completed. Now processing results...")
		except Exception as e:
			print(f"Error during fuzzy matching: {e}")
			return []

		# Collect all matches above threshold for each customer
		all_matches = []
		try:
			with ThreadPoolExecutor() as executor:
				futures = {
					executor.submit(
						process_customer_match,
						idx,
						row,
						scores,
						threshold,
						temp_names_clean,
						clean_to_source
					): idx
					for idx, row in customers_df.iterrows()
				}
				print("processing results...")

				for future in as_completed(futures):
					try:
						all_matches.extend(future.result())
					except Exception as e:
						print(f"Error in thread: {e}")

			print(f"Total fuzzy matches above threshold: {len(all_matches)}")

		except Exception as e:
			print(f"Error collecting matches: {e}")
			return []

		# --- Bulk insert only filtered matches ---
		bulk_objs = []
		for match in all_matches:
			try:
				bulk_objs.append(
					rica_watchlist_matches(
						ricaCustomerName=match.get('Customer Name', ''),
						ricaWatchlistMatch=match.get('Watchlist Match', ''),
						ricaMatchScore=float(match.get('match_score', 0)),
						ricaSource=match.get('ricaSource', ''),
						ricaAccountNumber=match.get('account_number', ''),
						ricaBranchCode=match.get('branch_code', ''),
						ricaOpenedBy=match.get('opened_by', '')
					)
				)
			except Exception as e:
				print(f"Error preparing match for bulk insert: {e}")

		if bulk_objs:
			try:
				rica_watchlist_matches.objects.bulk_create(bulk_objs)
				print(f"Inserted {len(bulk_objs)} matches into rica_watchlist_matches.")
			except Exception as e:
				print(f"Error during bulk_create: {e}")

		# --- Build DataFrame and grouped_matches from filtered matches ---
		try:
			matches_df = pd.DataFrame(all_matches)
			matches_df = matches_df.sort_values(by=['Customer Name', 'match_score'], ascending=[True, False])
			source_summary = (
				matches_df.groupby('ricaSource')
				.size()
				.reset_index(name='count')
				.sort_values(by='count', ascending=False)
				.rename(columns={'ricaSource': 'Source'})
			)
			source_summary_list = source_summary.to_dict(orient='records')
		except Exception as e:
			print(f"Error creating matches DataFrame: {e}")
			return []

		print(f"\n=== MATCHED CUSTOMERS GROUPED BY CUSTOMER ===")
		grouped_matches = []
		try:
			if not matches_df.empty:
				for customer_name, group in matches_df.groupby('Customer Name'):
					customer_matches = []
					for _, row in group.iterrows():
						customer_matches.append({
							'watchlist_match': row['Watchlist Match'],
							'source': row['Source'] if 'Source' in row else row['ricaSource'],
							'match_score': row['match_score'] if isinstance(row['match_score'], str) and row['match_score'].endswith('%') else f"{int(round(row['match_score']))}%"
						})
					grouped_matches.append({
						'customer_name': customer_name,
						'matches': customer_matches
					})
			else:
				print("No matches found.")
			print(f"Total matches: {len(matches_df)}")
		except Exception as e:
			print(f"Error grouping matches: {e}")

		# --- Format for Excel output and send email ---
		try:
			excel_df = matches_df.copy()
			excel_df['match_score'] = excel_df['match_score'].apply(lambda x: f"{int(round(float(x.strip('%'))))}%" if isinstance(x, str) and x.endswith('%') else f"{int(round(x))}%")
			excel_df = excel_df.sort_values(by=['Customer Name', 'match_score'], ascending=[True, False])
			excel_df.reset_index(drop=True, inplace=True)
			prev_name = None
			for idx, row in excel_df.iterrows():
				if row['Customer Name'] == prev_name:
					excel_df.at[idx, 'Customer Name'] = ''
					excel_df.at[idx, 'branch_code'] = ''
					excel_df.at[idx, 'opened_by'] = ''
					excel_df.at[idx, 'Account Number'] = ''
				else:
					prev_name = row['Customer Name']
			excel_df.insert(0, 'S/N', range(1, len(excel_df) + 1))
			excel_df = excel_df.rename(columns={
				'Customer Name': 'Customer Name',
				'Watchlist Match': 'Watchlist Match',
				'ricaSource': 'Source',
				'account_number': 'Account Number',
				'branch_code': 'Branch Code',
				'opened_by': 'Opened By',
				'match_score': 'Match Score'
			})
		except Exception as e:
			print(f"Error formatting Excel output: {e}")

		if grouped_matches:
			try:
				send_to = get_emails()
				print(f"Email will be sent to: {send_to}")
				options = {
					'grouped_matches': grouped_matches,
					'total_customers': len(customers_df),
					'total_matches': len(matches_df),
					'affected_customers': len(grouped_matches),
					'source_summary': source_summary_list
				}
				excel_filename = f"bank_customer_sanctions_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
				excel_path = os.path.join(os.getcwd(), excel_filename)
				excel_df.to_excel(excel_path, index=False)
				print(f"Excel file created: {excel_path}")
				send_response_mail(send_to, options, attachment_paths=[excel_path])
			except Exception as e:
				print(f"Error sending email or saving Excel file: {e}")
		print(len(grouped_matches), "matches grouped by customer.")
		return grouped_matches
	except Exception as e:
		print(f"General error in fuzzy_match_customers: {e}")
		return []