import sys

if ((len(sys.argv) < 3)):
        print("Wrong number of parameters. Should be 2 files for which result will be difference between second and first.")
        sys.exit(1)

# first file - first set
A = set(open(sys.argv[1]).read().split())
# second file - second set
B = set(open(sys.argv[2]).read().split())

# result - difference between second set and first set
for x in B.difference(A):
        print(x)



