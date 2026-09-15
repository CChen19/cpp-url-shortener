#ifndef SHORT_URL_HANDLER_H
#define SHORT_URL_HANDLER_H

class Config;

void init_short_url_handler(const Config& cfg);
void register_short_url_routes();

#endif
