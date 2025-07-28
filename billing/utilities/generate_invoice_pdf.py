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


def generate_pdf_invoice(order_list, amount, cash_total, total, restaurant, location, adjustments=0, adjustments_note=None):
    orders = reshape_list(order_list)
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('template.html')
    date = datetime.datetime.now().astimezone(
        pytz_timezone('Etc/GMT+7')).date()

    data = {
        'reference_no': 'CCDR#28756',
        'date': date,
        'orders_list': orders,
        'stripe': amount,
        'stripe_with_adjustments': float(amount) + adjustments,
        'cash': cash_total,
        'total': total,
        'restaurant': restaurant,
        'location': location,
        # 'logo': '',
        'adjustments': adjustments,
        'adjustments_note': adjustments_note
    }

    html_content = template.render(data).encode('utf-8')
    # Replace with the actual path if different
    path_to_wkhtmltopdf = '/usr/bin/wkhtmltopdf'
    # path_to_wkhtmltopdf = '/usr/local/bin/wkhtmltopdf'
    # test comment
    config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)
    options = {
        'orientation': 'Landscape',  # Change to landscape orientation
        'page-size': 'A4'  # You can adjust the page size as needed
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
