## Synopsis

Console application for sending messages to Kafka topic

Can send messages from directory(recursively or not) or from one file

Can parse files with chosen encoding by -e program argument

## Example and instruction

usage: java kafka-console-producer-*.jar [-d <directory>] [-e <encoding>] [-r]
       [-s <server>] -t <topic>
       
Options
       
  -d,--directory <directory>   Directory with xml files (current directory by
                               default), if set '.'(dot) then search files 
                               in current directory
       
  -e,--encoding <encoding>     Encoding of files (UTF-8 by default)
       
  -r,--recursively             Look for files recursively
       
  -s,--server <server>         Server path (localhost:9092 by default)
       
  -t,--topic <topic>           Kafka topic
       
  -h, --help                   For getting help message

## Russian instruction

Консольное приложение для отправки в Kafka всех файлов из выбранной директории

Опции

  -d,--directory <directory>   Папка с xml файлами (текущая папка по-умолчанию), 
                                '.'(точка) означает "брать все пакеты из папок и 
                                подпапок
       
  -e,--encoding <encoding>     кодировка, в которой пакеты лежат 
                                (UTF-8 по дефолту, поэтому можно не указывать)
       
  -r,--recursively             Обходит папки рекурсивно
  
  -s,--server <server>         Адрес сервера (localhost:9092 по-умолчанию)
       
  -t,--topic <topic>           Имя кафка топика

