/*
 * eagle-owl application.
 *
 * Copyright (C) 2012 Philippe Cornet <phil.cornet@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 */

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <usb.h>
#include <pthread.h>
#include <fcntl.h>
#include <time.h>
#include <mosquitto.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "cm160.h"
#include "usb_utils.h"
#include "db.h"
#include "demonize.h"

static char ID_MSG[11] = { 
      0xA9, 0x49, 0x44, 0x54, 0x43, 0x4D, 0x56, 0x30, 0x30, 0x31, 0x01 };
static char WAIT_MSG[11] = { 
      0xA9, 0x49, 0x44, 0x54, 0x57, 0x41, 0x49, 0x54, 0x50, 0x43, 0x52 };


#define HISTORY_SIZE 65536 // 30 * 24 * 60 = 43200 theoric history size

#define CP210X_IFC_ENABLE       0x00
#define CP210X_GET_LINE_CTL     0x04
#define CP210X_SET_MHS          0x07
#define CP210X_GET_MDMSTS       0x08
#define CP210X_GET_FLOW         0x14
#define CP210X_GET_BAUDRATE     0x1D
#define CP210X_SET_BAUDRATE     0x1E

/* CP210X_IFC_ENABLE */
#define UART_ENABLE             0x0001
#define UART_DISABLE            0x0000

struct cm160_device g_devices[MAX_DEVICES];
static unsigned char history[HISTORY_SIZE][11];
static char *mqtt_host;
static char *mqtt_topic = "cm160";
static int mqtt_port = 1883;
static time_t mqtt_reconnect = 0;
static struct mosquitto *mqtt = NULL;

// Mosquitto callbacks
void connect_callback(struct mosquitto *mqtt, void *obj, int result) {
  printf("connect callback, rc=%d\n", result);
  if (result == 0) {
    mqtt_reconnect = 0;
  }
}

// Data processing
static void process_live_data(struct record_data *rec) 
{
  static double _watts = -1;
  double w = rec->watts;

  if(!rec->isLiveData) // special case: update only the time
  {
    if(_watts == -1)
      return;
    w = _watts;
  }
  else
    _watts = w;

  FILE *fp =  fopen(".live", "w");
  if(fp)
  {
    if(rec->hour!=255) // to avoid writing strange values (i.e. date 2255, hour 255:255) that sometimes I got
      fprintf(fp, "%02d/%02d/%04d %02d:%02d - %.02f kW\n", rec->day, rec->month, rec->year, rec->hour, rec->min, w);
    fclose(fp);
  }
}

static void publish(struct record_data *rec) {
  static char buf[80];
  static struct tm tm;
  memset(&tm, 0, sizeof(struct tm));
//  tm.tm_sec = rec->sec;
  tm.tm_min = rec->min;
  tm.tm_hour = rec->hour;
  tm.tm_mday = rec->day;
  tm.tm_mon = rec->month - 1;
  tm.tm_year = rec->year - 1900;
  time_t t = mktime(&tm);       // local time
  if (rec->isLiveData) {
      t = time(NULL);   // why would I care about time on device?
  }
  snprintf(buf, sizeof(buf), "{\"type\":\"%s\",\"watts\":%1f,\"when\":%ld}", rec->isLiveData ? "live": "hist", rec->watts, t);
  mosquitto_publish(mqtt, NULL, mqtt_topic, strlen(buf), buf, 0, 0);
}

static void decode_frame(unsigned char *frame, struct record_data *rec)
{
  int volt = 230; // TODO: use the value from energy_param table (supply_voltage)
  rec->addr = 0; // TODO: don't use an harcoded addr value for the device...
  rec->year = frame[1] + 2000;
  rec->month = frame[2];
  rec->day = frame[3];
  rec->hour = frame[4];
  rec->min = frame[5];
  rec->cost = (frame[6]+(frame[7]<<8))/100.0;
  rec->amps = (frame[8]+(frame[9]<<8))*0.07; // mean intensity during one minute
  rec->watts = rec->amps * volt; // mean power during one minute
  rec->ah = rec->amps/60; // -> we must devide by 60 to convert into ah and wh
  rec->wh = rec->watts/60;
  rec->isLiveData = (frame[0] == FRAME_ID_LIVE)? true:false;
}

// Insert history into DB worker thread
void insert_db_history(void *data)
{
  int i;
  int num_elems = (int)data;
  // For an unknown reason, the cm160 sometimes sends a value > 12 for month
  // -> in that case we use the last valid month received.
  static int last_valid_month = 0; 
  printf("insert %d elems\n", num_elems);
  printf("insert into db...\n");
  clock_t cStartClock = clock();

  db_begin_transaction();
  for(i=0; i<num_elems; i++)
  {
    unsigned char *frame = history[i];
    struct record_data rec;
    decode_frame(frame, &rec);

    if(rec.month < 0 || rec.month > 12)
      rec.month = last_valid_month;
    else
      last_valid_month = rec.month;

    db_insert_hist(&rec);
    printf("\r %.1f%%", min(100, 100*((double)i/num_elems)));
  }
  db_update_status();
  db_end_transaction();

  printf("\rinsert into db... 100%%\n");
  printf("update db in %4.2f seconds\n", 
         (clock() - cStartClock) / (double)CLOCKS_PER_SEC);
}

bool receive_history = true;
int frame_id = 0;

static int process_frame(int dev_id, unsigned char *frame)
{
  int i;
  unsigned char data[1];
  unsigned int checksum = 0;
  static int last_valid_month = 0; 
  usb_dev_handle *hdev = g_devices[dev_id].hdev;
  int epout = g_devices[dev_id].epout;
//  for (i=0;i<11;i++) {
//    printf("%02x ", frame[i]);
//  }
//  printf("\n");

  if(strncmp((char *)frame, ID_MSG, 11) == 0)
  {
    printf("received ID MSG\n");
    data[0]=0x5A;
    usb_bulk_write(hdev, epout, (const char *)&data, sizeof(data), 1000);
  }
  else if(strncmp((char *)frame, WAIT_MSG, 11) == 0)
  {
    printf("received WAIT MSG\n");
    data[0]=0xA5;
    usb_bulk_write(hdev, epout, (const char *)&data, sizeof(data), 1000);
  }
  else
  {
    if(frame[0] != FRAME_ID_LIVE && frame[0] != FRAME_ID_DB)
    {
//      printf("data error: invalid ID 0x%x\n", frame[0]);
      return -1;
    }

    for(i=0; i<10; i++) {
      checksum += frame[i];
    }
    checksum &= 0xff;
    if(checksum != frame[10])
    {
//      printf("data error: invalid checksum: expected 0x%x, got 0x%x\n", frame[10], checksum);
      return -1;
    }

    struct record_data rec;
    decode_frame(frame, &rec);

    if(rec.month < 0 || rec.month > 12) {
      rec.month = last_valid_month;
    } else {
      last_valid_month = rec.month;
    }

    if(frame[0]==FRAME_ID_DB)
    {
      if(receive_history && frame_id < HISTORY_SIZE)
      {
        if(frame_id == 0) {
          printf("downloading history...\n");
        }
        else if(frame_id%10 == 0)
        { // print progression status
          // rough estimation : we should received a month of history
          // -> 31x24x60 minute records
          printf("\r %.1f%%", min(100, 100*((double)frame_id/(31*24*60))));
        }
        // cache the history in a buffer, we will insert it in the db later.
        memcpy(history[frame_id++], frame, 11);
      }
      else
      {
        db_insert_hist(&rec);
        db_update_status();
        process_live_data(&rec); // the record is not live data, but we do that to
                                 // update the time in the .live file
                                 // (the cm160 send a DB frame when a new minute starts)
      }
    }
    else
    {
      if(receive_history)
      { // When we receive the first live data, 
        // we know that the history is totally downloaded
        printf("\rdownloading history... 100%%\n");
        receive_history = false;
        // Now, insert the history into the db
        pthread_t thread;
        pthread_create(&thread, NULL, (void *)&insert_db_history, (void *)frame_id);
      }
      
      process_live_data(&rec);
      printf("LIVE: %02d/%02d/%04d %02d:%02d : %f W\n",
             rec.day, rec.month, rec.year, rec.hour, rec.min, rec.watts);
      publish(&rec);
    }
  }
  return 0;
}

static int io_loop(int dev_id)
{
  int ret;
  usb_dev_handle *hdev = g_devices[dev_id].hdev;
  int epin = g_devices[dev_id].epin;
  unsigned char buffer[512];
  unsigned char word[11];
  int j = -1;

  memset(buffer, 0, sizeof(buffer));
  memset(word, 0, sizeof(word));

  while(1) {
    memset(buffer, 0, sizeof(buffer));
    ret = usb_bulk_read(hdev, epin, (char*)buffer, sizeof(buffer), 10000);
    if(ret < 0) {
      printf("bulk_read returned %d (%s)\n", ret, usb_strerror());
      return -1;
    }
    // Cycle through read bytes trying to find a complete frame.
    // This approach will automatically restore sync if we come in
    // halfway through a frame, which happens.
    for (int i=0;i<ret;i++) {
      unsigned char c = buffer[i];
      if (j < 10) {
        word[j++] = c;
      } else if (j == 10) {
        word[j++] = c;
        if (!process_frame(dev_id, word)) {
          j = 0;
        }
      } else {
        for (int k=0;k<10;k++) {
          word[k] = word[k + 1];
        }
        word[10] = c;
        if (!process_frame(dev_id, word)) {
          j = 0;
        }
      }
    }

    if (mqtt_reconnect == 0) {
      int rc = mosquitto_loop(mqtt, -1, 1);
      if (rc) {
        printf("Lost MQTT connection to \"%s\", retrying in 10s", mqtt_host);
        mqtt_reconnect = time(NULL) + 10;
      }
    } else if (time(NULL) > mqtt_reconnect) {
      mqtt_reconnect = time(NULL) + 10;
      mosquitto_reconnect(mqtt);
    }
  }
  return 0;
}

static int handle_device(int dev_id)
{
  int r, i;
  struct usb_device *dev = g_devices[dev_id].usb_dev;
  usb_dev_handle *hdev = g_devices[dev_id].hdev;

  usb_detach_kernel_driver_np(hdev, 0);
  
  if( 0 != (r = usb_set_configuration(hdev, dev->config[0].bConfigurationValue)) )
  {
    printf("usb_set_configuration returns %d (%s)\n", r, usb_strerror());
    return -1;
  }

  if((r = usb_claim_interface(hdev, 0)) < 0)
  {
    printf("Interface cannot be claimed: %d\n", r);
    return r;
  }

  int nep = dev->config->interface->altsetting->bNumEndpoints;
  for(i=0; i<nep; i++)
  {
    int ep = dev->config->interface->altsetting->endpoint[i].bEndpointAddress;
    if(ep&(1<<7))
      g_devices[dev_id].epin = ep;
    else
      g_devices[dev_id].epout = ep;
  }

  // Set baudrate
  int baudrate = 250000;
  r = usb_control_msg(hdev, USB_TYPE_VENDOR | USB_RECIP_INTERFACE | USB_ENDPOINT_OUT, 
                      CP210X_IFC_ENABLE, UART_ENABLE, 0, NULL, 0, 500);
  r = usb_control_msg(hdev, USB_TYPE_VENDOR | USB_RECIP_INTERFACE | USB_ENDPOINT_OUT, 
                      CP210X_SET_BAUDRATE, 0, 0, (char *)&baudrate, sizeof(baudrate), 500);
  r = usb_control_msg(hdev, USB_TYPE_VENDOR | USB_RECIP_INTERFACE | USB_ENDPOINT_OUT, 
                      CP210X_IFC_ENABLE, UART_DISABLE, 0, NULL, 0, 500);
  
// read/write main loop
  io_loop(dev_id);
 
  usb_release_interface(hdev, 0);
  return 0;
}


void usage(char *host) {
  fprintf(stderr, "Usage: %s [-d] [--mqtt-host <host>] [--mqtt-port <port>] [--mqtt-client <name>]\n", host);
  exit(1);
}

int main(int argc, char **argv)
{
  int dev_cnt;
  bool daemonize = false;
  char *mqtt_clientid = "cm160";

  for (int i=1;i<argc;i++) {
    if (!strcmp(argv[i], "-d")) {
      daemonize = true;
    } else if (!strcmp(argv[i], "--mqtt-host")) {
      mqtt_host = argv[++i];
    } else if (!strcmp(argv[i], "--mqtt-port")) {
      mqtt_port = atoi(argv[++i]);
    } else if (!strcmp(argv[i], "--mqtt-client")) {
      mqtt_clientid = argv[++i];
    } else if (!strcmp(argv[i], "--mqtt-topic")) {
      mqtt_topic = argv[++i];
    } else {
      usage(argv[0]);
    }
  }

  if (daemonize) {
    demonize(argv[0]);
  }
  if (mqtt_host != NULL) {
    mqtt = mosquitto_new(mqtt_clientid, true, 0);
    if (!mqtt) {
      perror("Unable to allocate");
      exit(1);
    }
    mosquitto_connect_callback_set(mqtt, connect_callback);
    mosquitto_connect(mqtt, mqtt_host, mqtt_port, 60);
  }

  while(1)
  {
    db_open();
    dev_cnt = 0;
    receive_history = true;
    frame_id = 0;
    printf("Wait for cm160 device to be connected\n");
    while((dev_cnt = scan_usb()) == 0) {
      sleep(2);
    }
    printf("Found %d compatible device%s\n", dev_cnt, dev_cnt>1?"s":"");

    // Only 1 device supported
    if(!(g_devices[0].hdev = usb_open(g_devices[0].usb_dev)))
    {
      fprintf(stderr, "failed to open device\n");
      db_close();
      break;
    }
    handle_device(0); 
    usb_close(g_devices[0].hdev);
    db_close();
  }

  return 0;
}

