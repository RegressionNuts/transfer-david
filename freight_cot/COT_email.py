import win32com.client as win32
import matplotlib.pyplot as plt
import pandas as pd
from io import BytesIO
import base64
import holidays

# Configuration
EMAIL_CONFIG = {
    'recipient_email': ['seafreight.chart@olamagri.com'],  
    # 'recipient_email': ['yuhang.hou@olam-agri.com'],  # Can be string or list
    'subject': 'EEX and SGX COT data',
    'cc_emails': ['david.li@olam-agri.com','francisco.fructuoso@olam-agri.com'],  
}

def generate_plots_and_tables(symbol,include_eex,normalize_size):

    cot_data_sgx = pd.read_csv('freight_cot/data/SGX_COT.csv')
    cot_data_sgx['date'] = pd.to_datetime(cot_data_sgx['Clear Date'])
    cot_data_sgx = cot_data_sgx[cot_data_sgx['Symbol'] == symbol]
    cot_data_sgx = cot_data_sgx.set_index('date')
    cot_data_sgx.fillna(0, inplace=True)

    col_list = [  'Open Interest', 'Physicals Long', 'Physicals Short', 'Managed Money Long', 'Managed Money Short', 'Financial Institutions Long', 'Financial Institutions Short']
    cot_data = cot_data_sgx[col_list].copy()
    if include_eex:
        cot_data_eex = pd.read_csv('freight_cot/data/EEX_COT.csv')
        cot_data_eex['date'] = pd.to_datetime(cot_data_eex['Clear Date'])
        cot_data_eex = cot_data_eex[cot_data_eex['Symbol'] == symbol]
        diff_date = set(cot_data_sgx.index)-set(cot_data_eex['date'])
        print(diff_date )
        date_map = dict()
        for _date in diff_date:
            print(holidays.DE(_date.year)[_date.date()])
            if holidays.DE(_date.year)[_date.date()] in ['Karfreitag','Good Friday']:
                previous_day = _date - pd.Timedelta(days=1)
                date_map[previous_day] = _date
        print(date_map)
        cot_data_eex['date'] = cot_data_eex['date'].apply(lambda x: date_map.get(x, x))
        if set(cot_data_sgx.index)-set(cot_data_eex['date']) != set():
            print(set(cot_data_sgx.index)-set(cot_data_eex['date']) )
            raise Exception('date not match')
            
        cot_data_eex = cot_data_eex.set_index('date')
        cot_data_eex.fillna(0, inplace=True)
        
        cot_data_eex = cot_data_eex[col_list].copy()
        cot_data +=cot_data_eex
        cot_data.dropna(inplace=True)
        if len(cot_data) != len(cot_data_sgx):
            raise Exception('data not match')
    
    cot_data.fillna(0, inplace=True)
    cot_data['MM Net'] = cot_data['Managed Money Long'] - cot_data['Managed Money Short']
    cot_data['MM Ratio'] = cot_data['MM Net'] / cot_data['Open Interest']
    cot_data['FI Net'] = cot_data['Financial Institutions Long'] - cot_data['Financial Institutions Short']
    cot_data['FI Ratio'] = cot_data['FI Net'] / cot_data['Open Interest']
    cot_data['P Net'] = cot_data['Physicals Long'] - cot_data['Physicals Short']
    cot_data['P Ratio'] = cot_data['P Net'] / cot_data['Open Interest']
    cot_data['MM ZScore'] = (cot_data['MM Ratio'] - cot_data['MM Ratio'].rolling(26).mean()) / cot_data['MM Ratio'].rolling(26).std(ddof=0)
    cot_data['P ZScore'] = (cot_data['P Ratio'] - cot_data['P Ratio'].rolling(26).mean()) / cot_data['P Ratio'].rolling(26).std(ddof=0)
    cot_data['FI ZScore'] = (cot_data['FI Ratio'] - cot_data['FI Ratio'].rolling(26).mean()) / cot_data['FI Ratio'].rolling(26).std(ddof=0)

    front_month = pd.read_csv(f'./data/series/{symbol}/{symbol}_1_3.csv')
    front_month['date'] = pd.to_datetime(front_month['date'])
    front_month.set_index('date', inplace=True)
    front_month.rename(columns={'close':'front_month'}, inplace=True)

    front_quarter = pd.read_csv(f'./data/series/{symbol}/{symbol}Q_0_3.csv')
    front_quarter['date'] = pd.to_datetime(front_quarter['date'])
    front_quarter.set_index('date', inplace=True)
    front_quarter.rename(columns={'close':'front_quarter'}, inplace=True)
    front_month['front_quarter'] = front_quarter['front_quarter']
    front_month = front_month.loc['2023-01-01':]
    cot_data['front_month'] = front_month['front_month']
    cot_data['front_quarter'] = front_month['front_quarter']
    cot_data.dropna(inplace=True)
    if normalize_size:
        cols_filter = ['MM ZScore', 'P ZScore', 'FI ZScore','front_month','front_quarter']
    else:
        cols_filter = ['MM Net', 'P Net', 'FI Net','front_month','front_quarter']
    fig = plot_data(cot_data,front_month,symbol,include_eex,normalize_size)
    cot_data = cot_data[cols_filter]
    return fig, cot_data.reset_index().tail(4)
    
def plot_data(cot_data,price_data, symbol,include_eex,normalize):
    fig, ax1 = plt.subplots(figsize = (8, 4))

    ax1.set_xlabel('time')
    ax1.set_ylabel('COT Info')
    if normalize:
        ax1.plot(cot_data['MM ZScore'] , color='blue', label='MM ZScore')
        ax1.plot(cot_data['P ZScore'], color='green', label='P ZScore')
        ax1.plot(cot_data['FI ZScore'] , color='red', label='FI ZScore')
    else:
        ax1.plot(cot_data['MM Net'], color='blue', label='MM Net')
        ax1.plot(cot_data['P Net'], color='green', label='P Net')
        ax1.plot(cot_data['FI Net'], color='red', label='FI Net')
    ax1.legend(loc='upper left',bbox_to_anchor=(0, 1))  
    ax1.tick_params(axis='y')

    ax2 = ax1.twinx()
    
    ax2.set_ylabel('FFA Price')
    ax2.plot(price_data['front_month'], color='#C9A0DC', label='Front Month')
    ax2.plot(price_data['front_quarter'], color= '#FFD700', label='Front Quarter')
    ax2.legend(loc='center left',bbox_to_anchor=(0, 0.7))  
    ax2.tick_params(axis='y')
    fig.tight_layout()
    if include_eex:
        tail = ' SGX + EEX'
    else:
        tail = ' SGX'
    if normalize:
        tail += ' with position size normalized'
    title = f' COT data for {symbol}{tail}'
    plt.title(title, fontsize=16)
    return fig

def plot_to_base64(fig):
    """Convert matplotlib figure to base64 encoded image"""
    buffer = BytesIO()
    fig.savefig(buffer, format='png', dpi=80, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close(fig) 
    return image_base64

def dataframe_to_html(df):
    """Convert pandas DataFrame to styled HTML table"""
    return df.to_html(index=False, classes='dataframe', border=1, justify='center',float_format="{:,.2f}".format)

def create_email_body_with_embedded_images(plots_base64, tables_html):
    """Create HTML email body with embedded images"""
    
    html_body = f"""
    <html>
    <head>
        <style>
            body {{ 
                font-family: Calibri, Arial, sans-serif; 
                margin: 20px; 
                line-height: 1.6;
            }}
            .section {{ 
                margin-bottom: 40px; 
                padding: 20px; 
                border: 1px solid #E0E0E0; 
                background-color: #FAFAFA;
                border-radius: 5px;
            }}
            .plot-container {{ 
                text-align: center; 
                margin: 20px 0;
                padding: 15px;
                background-color: white;
                border: 1px solid #DDD;
                border-radius: 3px;
            }}
            .table-container {{ 
                margin: 20px 0;
                overflow-x: auto;
            }}
            table {{ 
                border-collapse: collapse; 
                width: 100%; 
                font-size: 10pt;
                margin: 10px 0;
            }}
            th {{ 
                background-color: #2E75B6; 
                color: white; 
                padding: 10px; 
                text-align: center;
                font-weight: bold;
            }}
            td {{ 
                padding: 8px; 
                border: 1px solid #DDD;
                text-align: center;
            }}
            tr:nth-child(even) {{ 
                background-color: #F8F8F8; 
            }}
            tr:hover {{
                background-color: #F0F0F0;
            }}
            h1 {{ 
                color: #2E75B6; 
                border-bottom: 3px solid #2E75B6;
                padding-bottom: 10px;
            }}
            h2 {{ 
                color: #4472C4; 
                background-color: #E6F0FF;
                padding: 10px;
                border-left: 4px solid #4472C4;
            }}
            h3 {{ 
                color: #5B9BD5;
                margin-top: 20px;
            }}
            .image-title {{
                font-weight: bold;
                color: #2E75B6;
                margin-bottom: 10px;
                font-size: 12pt;
            }}
            .footer {{
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #DDD;
                color: #666;
                font-size: 9pt;
            }}
        </style>
    </head>
    <body>
        <h1>EEX and SGX COT data against FFA Price</h1>
        <p>Dear Recipient,</p>
        <p>Please find below the COT analysis report.</p>
        
        {''.join([f'''
        <div class="section">
            <h2> Section {i+1}</h2>
            
            <div class="plot-container">
                <img src="data:image/png;base64,{plots_base64[i]}" 
                     alt="Plot {i+1}" 
                     style="max-width: 90%; height: auto; border: 1px solid #EEE;"
                     onerror="this.style.display='none'">
            </div>
            
            <div class="table-container">
                {tables_html[i]}
            </div>
        </div>
        ''' for i in range(8)])}
        
        <div class="footer">
            <p><strong>Best regards,</strong><br>Yuhang Hou</p>
            <p><em>Report generated on {pd.Timestamp.now().strftime('%A, %B %d, %Y at %H:%M:%S')}</em></p>
        </div>
    </body>
    </html>
    """
    return html_body

def send_email_with_embedded_images(recipient_email, subject, cc_emails=None, bcc_emails=None):
    """
    Send email with images embedded in the HTML content
    """
    try:
        plots_base64 = []
        tables_html = []
        
        i=0
        for symbol in ['C5TC','P4TC']:
            for include_eex in [0,1]:
                for normalize in [0,1]:
                    print(f"Generating plots and tables for {symbol}, include_eex={include_eex}, normalize={normalize}...")
                    fig,df = generate_plots_and_tables(symbol,include_eex,normalize)
                    plot_base64 = plot_to_base64(fig)
                    plots_base64.append(plot_base64)
                    tables_html.append(dataframe_to_html(df))
                    i+=1
            print(f"✅ Generated plot and table {i}/8")
        
        html_body = create_email_body_with_embedded_images(plots_base64, tables_html)
        
        outlook = win32.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)  
        mail.Subject = subject
        mail.HTMLBody = html_body
        
        if isinstance(recipient_email, list):
            for email in recipient_email:
                mail.Recipients.Add(email)
        else:
            mail.Recipients.Add(recipient_email)
        
        if cc_emails:
            if isinstance(cc_emails, list):
                for email in cc_emails:
                    mail.CC += email + ";"
            else:
                mail.CC = cc_emails
        
        mail.Display(True) 
        
        print("✅ Email opened in Outlook for review. Please check and click Send.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating email: {e}")
        return False

def send_automated_email(recipient_email, subject, cc_emails=None, bcc_emails=None):
    """
    Send email automatically without review
    """
    try:
        print("Generating plots and tables...")
        plots_base64 = []
        tables_html = []
        i=0
        for symbol in ['C5TC','P4TC']:
            for include_eex in [0,1]:
                for normalize in [0,1]:
                    fig,df = generate_plots_and_tables(symbol,include_eex,normalize)
                    plot_base64 = plot_to_base64(fig)
                    plots_base64.append(plot_base64)
                    tables_html.append(dataframe_to_html(df))
                    i+=1
                    print(f"✅ Generated plot and table {i}/8") 
            
        html_body = create_email_body_with_embedded_images(plots_base64, tables_html)
        
        outlook = win32.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)
        
        mail.Subject = subject
        mail.HTMLBody = html_body
        
        if isinstance(recipient_email, list):
            mail.To = ";".join(recipient_email)
        else:
            mail.To = recipient_email
        
        if cc_emails:
            if isinstance(cc_emails, list):
                mail.CC = ";".join(cc_emails)
            else:
                mail.CC = cc_emails
        
        mail.Send()
        print("✅ Email sent successfully!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        return False


# Main execution
if __name__ == "__main__":
    # 1: Open email in Outlook for review (Recommended)
    # success = send_email_with_embedded_images(
    #     recipient_email=EMAIL_CONFIG['recipient_email'],
    #     subject=EMAIL_CONFIG['subject'],
    #     cc_emails=EMAIL_CONFIG['cc_emails'],
    # )

    
    # 2: Uncomment below to send automatically without review
    success = send_automated_email(
        recipient_email=EMAIL_CONFIG['recipient_email'],
        subject=EMAIL_CONFIG['subject'],
        cc_emails=EMAIL_CONFIG['cc_emails'],
    )