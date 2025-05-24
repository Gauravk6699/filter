raw_stock_data = """Reliance Industries
Tata Consultancy Services (TCS)
Infosys
HDFC Bank
ICICI Bank
State Bank of India (SBI)
Hindustan Unilever
ITC
Larsen & Toubro (L&T)
Axis Bank
Kotak Mahindra Bank
Bajaj Finance
Bharti Airtel
Maruti Suzuki
NTPC
Power Grid Corporation
UltraTech Cement
Grasim Industries
Wipro
Tech Mahindra
HCL Technologies
Adani Enterprises
Adani Ports & SEZ
Tata Motors
Tata Steel
JSW Steel
Coal India
ONGC
HDFC Life
SBI Life Insurance
Divi's Laboratories
Dr. Reddy's Laboratories
Cipla
Sun Pharmaceutical
Nestle India
Britannia Industries
Asian Paints
Eicher Motors
Hero MotoCorp
Bajaj Auto
IndusInd Bank
Dabur India
Godrej Consumer Products
Pidilite Industries
Ambuja Cements
Shree Cement
M&M
Bank of Baroda
Canara Bank
PNB
Vodafone Idea
Zee Entertainment
DLF
InterGlobe Aviation (IndiGo)
Havells India
Siemens
Tata Power
Tata Elxsi
Mphasis
Persistent Systems
Page Industries
MRF
United Spirits
Berger Paints
Jubilant FoodWorks
Varun Beverages
AU Small Finance Bank
ICICI Prudential Life
ICICI Lombard
Cholamandalam Investment
REC Ltd
PFC Ltd
BHEL
BEL
IRCTC
TVS Motor
Voltas
GAIL
NMDC
Trent
Indraprastha Gas
Lupin
Torrent Pharma
Biocon
Alkem Labs
Apollo Hospitals
Max Healthcare
Zydus Lifesciences
Abbott India
Metropolis Healthcare
Gland Pharma
Polycab
Aditya Birla Fashion
Manappuram Finance
L&T Finance
Ujjivan Small Finance Bank
Federal Bank
IDFC First Bank
Bandhan Bank
Indiabulls Housing Finance
here is the list
"""

def clean_stock_names(text_data):
    # Split the text into individual lines
    lines = text_data.split('\n')

    # Remove any empty lines and trim whitespace
    cleaned_lines = [line.strip() for line in lines if line.strip()]

    # Remove the trailing line "here is the list"
    if cleaned_lines and cleaned_lines[-1] == "here is the list":
        cleaned_lines.pop()

    return cleaned_lines

if __name__ == "__main__":
    cleaned_stocks = clean_stock_names(raw_stock_data)
    print(cleaned_stocks)
