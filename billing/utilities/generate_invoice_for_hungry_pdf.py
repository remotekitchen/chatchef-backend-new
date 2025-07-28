import datetime
import os
from tempfile import NamedTemporaryFile

import pdfkit
from jinja2 import Environment, FileSystemLoader
from pytz import timezone as pytz_timezone
from xhtml2pdf import pisa


def reshape_list(arr, row_length=16):
    reshaped_arr = []
    for i in range(0, len(arr), row_length):
        reshaped_arr.append(arr[i:i + row_length])
    return reshaped_arr


def generate_pdf_for_hungry_invoice(order_list,
    amount,
    cash_total,
    total,
    sales_bkash,
    restaurant,
    location,
    adjustments=0,
    adjustments_note=None,
    total_amount_to_restaurant=None,
    commission_percentage=None,
    commission_amount=None,
    ht_profit=None,
    restaurant_discount=None,
    ht_discount=None,
    grand_total=None):
    orders = reshape_list(order_list)
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('templatehungry.html')
    date = datetime.datetime.now().astimezone(
        pytz_timezone('Etc/GMT+7')).date()
    
    print("orders-----999", orders)

    data = {
        'reference_no': 'CCDR#28756',
        'date': date,
        'orders_list': orders,
        'stripe': amount,
        'stripe_with_adjustments': float(amount) + adjustments,
        'cash': cash_total,
        'sales_bkash': sales_bkash,
        'total': total,
        'restaurant': restaurant,
        'location': location,
        'adjustments': adjustments,
        'adjustments_note': adjustments_note,
        'total_amount_to_restaurant': total_amount_to_restaurant,
        'commission_percentage': commission_percentage,
        'commission_amount': commission_amount,
        'ht_profit': ht_profit,
        'restaurant_discount': restaurant_discount,
        'ht_discount': ht_discount,
        'grand_total': grand_total
    }

    html_content = template.render(data).encode('utf-8')
    # Replace with the actual path if different
    # path_to_wkhtmltopdf = '/usr/local/bin/wkhtmltopdf'
    path_to_wkhtmltopdf = '/usr/bin/wkhtmltopdf'
    # test comment
    config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)
    options = {
        'orientation': 'Landscape',  # Change to landscape orientation
        'page-size': 'A4',
        'zoom': '0.75'  # You can adjust the page size as needed
    }
    with NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
        pdfkit.from_string(html_content.decode('utf-8'),
                           tmp_pdf.name, configuration=config, options=options)
        tmp_pdf.seek(0)
        pdf_content = tmp_pdf.read()

    # with NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
    #     pisa.CreatePDF(html_content, dest=tmp_pdf)
    #     tmp_pdf.seek(0)
    #     pdf_content = tmp_pdf.read()

    print("PDF generated successfully!")
    return pdf_content
