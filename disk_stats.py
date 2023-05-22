import gzip, pandas
from argparse import ArgumentParser as AP
import humanize


def stats(filename, top_n):
  with gzip.open(filename, mode='rt', newline="") as input:
    df = pandas.read_csv(input)
  df2 = df.groupby(['uid', 'gid'], sort=False)
  counts = df2['filename'].count().nlargest(top_n)
  sizes = df2['file_size'].sum().nlargest(top_n)
  total_size = df2['num_blocks'].sum().mul(512).nlargest(top_n)
  most_files = [
      "----------Users with most files------------",
      "{uid:>15s} {gid:>15s} {count:>30s}".format(uid="uid",
                                                  gid="gid",
                                                  count="Number of files")
  ]
  for p, c in counts.items():
    most_files.append("{uid:>15d} {gid:>15d} {count:>30s}".format(
        uid=p[0], gid=p[1], count=humanize.intcomma(c)))
  print("\n".join(most_files))

  most_volume = [
      "----------Users with most disk usage------------",
      "{uid:>15s} {gid:>15s} {count:>30s}".format(uid="uid",
                                                  gid="gid",
                                                  count="Total File Size")
  ]

  for p, c in sizes.items():
    most_volume.append("{uid:>15d} {gid:>15d} {count:>30s}".format(
        uid=p[0], gid=p[1], count=humanize.naturalsize(c, binary=True)))

  print("\n".join(most_volume))

  most_actual = [
      "----------Users with most actual disk usage------------",
      "{uid:>15s} {gid:>15s} {count:>30s}".format(uid="uid",
                                                  gid="gid",
                                                  count="Actual Disk Usage")
  ]

  for p, c in total_size.items():
    most_actual.append("{uid:>15d} {gid:>15d} {count:>30s}".format(
        uid=p[0], gid=p[1], count=humanize.naturalsize(c, binary=True)))

  print("\n".join(most_actual))


def parse_arguments():
  parser = AP("DUStats")
  parser.add_argument('-f', '--filename', default=None, type=str, required=True)
  parser.add_argument('-t', '--top-n', default=20, type=int)
  return parser.parse_known_args()


def main():
  args, unk = parse_arguments()
  stats(args.filename, args.top_n)


if "__main__" in __name__:
  main()
