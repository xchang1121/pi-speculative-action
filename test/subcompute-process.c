#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

int main(int argc, char **argv) {
  int trusted = argc == 2 && !strcmp(argv[1], "--trusted");
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  const char *socket_path = getenv("PI_SUBCOMPUTE_SOCKET");
  const char *context = getenv("PI_SUBCOMPUTE_CONTEXT");
  if (fd < 0 || !socket_path || !context ||
      strlen(socket_path) >= sizeof(address.sun_path))
    return 2;
  strcpy(address.sun_path, socket_path);
  if (connect(fd, (struct sockaddr *)&address, sizeof(address)))
    return 3;
  FILE *stream = fdopen(fd, "r+");
  if (!stream)
    return 4;
  if (trusted)
    fprintf(stream, "{\"op\":\"compute\",\"context\":\"%s\",\"namespace\":\"config.parse\",\"key\":null,"
                    "\"inputDigest\":\"a25b48de30e7b8f6c6b99c3b565f62521ec46bc75e82d6b93fc381efe3997e79\"}\n", context);
  else
    fprintf(stream,
          "{\"op\":\"compute\",\"context\":\"%s\","
          "\"namespace\":\"process-input\",\"key\":\"input\","
          "\"inputResources\":[{\"path\":\"input.txt\",\"scope\":\"content\"}],"
          "\"includeInputContent\":true}\n",
          context);
  fflush(stream);
  char *line = NULL;
  size_t size = 0;
  if (getline(&line, &size, stream) < 0)
    return 5;
  int miss = strstr(line, "\"state\":\"miss\"") != NULL;
  char token[37];
  char *id = strstr(line, "\"token\":\"");
  if ((trusted || miss) && (!id || sscanf(id, "\"token\":\"%36[0-9a-f-]", token) != 1))
    return 6;
  if (trusted && strstr(line, "\"state\":\"input\"")) {
    /* {"debug":"first","release":"second"} */
    fprintf(stream, "{\"op\":\"input\",\"token\":\"%s\",\"value\":{\"encoding\":\"base64\","
                    "\"value\":\"eyJkZWJ1ZyI6ImZpcnN0IiwicmVsZWFzZSI6InNlY29uZCJ9\"}}\n", token);
    fflush(stream);
    if (getline(&line, &size, stream) < 0) return 5;
  }
  char *value = strstr(line, "\"value\":\"");
  if (!value)
    return 6;
  value += strlen("\"value\":\"");
  char *end = strchr(value, '"');
  if (!end)
    return 7;
  *end = 0;
  if (miss) {
    fprintf(stream,
            "{\"op\":\"publish\",\"token\":\"%s\",\"durationMs\":1,"
            "\"value\":{\"encoding\":\"base64\",\"value\":\"%s\"}}\n",
            token, value);
    fflush(stream);
  }
  const char *alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  unsigned int bits = 0;
  int count = 0;
  for (const char *byte = value; *byte && *byte != '='; byte++) {
    const char *digit = strchr(alphabet, *byte);
    if (!digit)
      return 8;
    bits = (bits << 6) | (unsigned int)(digit - alphabet);
    count += 6;
    if (count >= 8) {
      count -= 8;
      putchar((bits >> count) & 255);
    }
  }
  if (trusted) {
    fprintf(stream, "{\"op\":\"consumed\",\"token\":\"%s\"}\n", token);
    fflush(stream);
  }
  if ((miss || trusted) && getline(&line, &size, stream) < 0)
    return 9;
  free(line);
  fclose(stream);
  return 0;
}
