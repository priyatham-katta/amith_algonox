FROM apache_base

# Copy over the apache configuration file and enable the site
COPY ./stats.conf /etc/apache2/sites-available/stats.conf
COPY ./httpd.conf /etc/apache2/httpd.conf
RUN echo "Include /etc/apache2/httpd.conf" >> /etc/apache2/apache2.conf
COPY ./mpm_event.conf /etc/apache2/mods-available/mpm_event.conf

RUN a2ensite stats
RUN a2enmod headers

# Copy over the wsgi file
COPY ./stats.wsgi /var/www/stats/stats.wsgi

RUN chmod a+x /var/www/stats/stats.wsgi

COPY ./run.py /var/www/stats/run.py
COPY ./app /var/www/stats/app/

RUN a2dissite 000-default.conf
RUN a2ensite stats.conf

EXPOSE 80

WORKDIR /var/www/stats

CMD  /usr/sbin/apache2ctl -D FOREGROUND

