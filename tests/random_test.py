import random
import string
import csv
import csvdiff3.merge3

class RandomFile:
    """
    Class that generates a virtual random file of (key,number) pairs in
    CSV format.
    """
    def __init__(self, file_to_copy = None):

        # create a local random number generator just for this
        # instance

        self.random_generator = random.Random()

        # and seed that RNG.  If we are copying an existing RandomFile
        # object, copy its seed too so that the entire file's virtual
        # contents will be reproduced.

        if file_to_copy:
            self.random_generator.setstate(file_to_copy.start_state)
        else:
            self.random_generator.seed()

        # And save the initial RNG seed so that we can clone it later
        # if necessary.

        self.start_state = self.random_generator.getstate()

        # Instantiate the generator function for the file's contents.
        #
        # We do this at the creation of the file, and again whenever
        # we seek() back to the start of the file.

        self.contents = self._generate_contents()
        self.eof = False

    def clone(self):

        # We cannot maintain two different seek() pointers into the
        # same RandomFile, as that would require maintaining multiple
        # distinct states for the random number generator.  So always
        # clone a complete copy of the object when we need multiple
        # streams.

        return RandomFile(self)

    def __iter__(self):

        # Iteraterating over the file simply executes the class's main
        # generator function

        return self.contents

    def random_key(self):

        # Generate a random key for the CSV file, using the object's
        # local RNG.  Creates a simple random string of 6 characters
        # randomly chosen from upper/lowercase alphanumeric
        # characters.

        return ''.join(
            self.random_generator.choices(
                string.ascii_uppercase
                + string.ascii_lowercase
                + string.digits, k=6))

    def _generate_contents(self):
        """
        Generator function for the random file:
        returns a series of randomised lines
        """

        # Simple CSV format: start with the header (remembering the
        # end-of-line char)

        yield "name,number\n"

        # and continue with 10,000 (key,number) lines

        for n in range(0,10000):
            yield f"{self.random_key()},{n}\n"

    def readline(self):
        """
        Readline function simply returns the next line from the generator
        """
        try:
            return next(self.contents)
        except StopIteration:
            return ""

    def read(self, length):
        """
        Also provide a buffered read() function to allow the csvmerge
        dump-on-failure handler to run shutil.copyfileobj() on the
        RandomFile
        """
        buffer = getattr(self, "buffer", None)
        if not buffer:
            # Fetch a new line if we don't already have any content
            buffer = self.readline()
        buflen = len(buffer)

        if buflen:
            # Return as much of the current line as will fit into the request
            retlen = max(buflen, length)
            result = buffer[:retlen]
            buffer = buffer[retlen:]
        else:
            # At EOF we just return nothing.
            result = ''
            buffer = None

        self.buffer = buffer
        return result

    def seek(self, offset, whence = 0):
        """Seek to a new file offset"""

        # Only support seeking directly back to the start of the file
        assert whence == 0
        assert offset == 0

        # And when we do so, reset the RNG internal state:
        self.random_generator.setstate(self.start_state)

        # and restart the generator function.
        self.contents = self._generate_contents()

class TweakedRandomFile(RandomFile):
    """
    Class that takes a CSV stream as input, and randomly perturbs it
    for testing.
    """

    def __init__(self, subfile):
        # Instantiate the RandomFile superclass to set the initial RNG state
        RandomFile.__init__(self)

        # and set up a new generator instance for the perturbed file's
        # contents
        self.subfile = subfile
        self.contents = self._modify_contents()

    def clone(self):
        # Cloning the perturbed stream requires us to clone the input
        # stream too, as we need an independent iterator over that
        # stream.
        new_subfile = self.subfile.clone()

        # Then clone this object, to make sure we inherit the RNG initial state
        new_file = TweakedRandomFile(new_subfile)
        new_file.random_generator.setstate(self.start_state)
        return new_file

    def random(self, maxint):
        # We'll be perturbing the file randomly so create a simple
        # helper to generate a random number in the range [1,maxint]
        # from the local RNG instance, to help drive n-in-maxint choices.
        return self.random_generator.randint(1,maxint)

    def _modify_contents(self):
        """Generator function for the perturbed file contents."""

        # Maintain a stack of lines we want to move around in the file
        # during perturbation.  We will add lines to the stack at
        # random as we encounter them, and pick existing lines off the
        # stack also at random.
        stack = []

        # Get a fresh iterator over the input file we are perturbing
        self.subfile.seek(0)
        lines = self.subfile.__iter__()

        # First line: always return the header line intact
        yield next(lines)

        for line in lines:
            # First decide if we are going to insert any
            # previously-stashed lines, before we decide what to do
            # with this new line

            while self.random(5) == 1:
                if not stack:
                    break

                n = self.random_generator.randint(0,len(stack)-1)
                if self.random(4) == 1:
                    # Either return this line and keep it for later too
                    yield stack[n]
                else:
                    # Or return it just once.
                    yield stack.pop(n)

            choice = self.random_generator.randint(1,12)
            # Apply possible changes to the file at random:
            if choice == 1:
                # Delete this line
                pass
            elif choice == 2:
                # Save this line to bring back elsewhere in the file:
                stack.append(line)
            elif choice == 3:
                # Duplicate this line: repeat it here but also insert later
                yield line
                stack.append(line)
            elif choice <= 5:
                # Reproduce the line, but with a different value
                words = line.split(",")
                number = self.random_generator.randint(1,10000)
                words[1] = str(number)+"\n"
                line = ",".join(words)
                yield line
            elif choice == 6:
                # Replace the key with a blank
                words = line.split(",")
                words[0] = ''
                line = ",".join(words)
                yield line
            else:
                # Otherwise just return this line unmodified.
                yield line

        for line in stack:
            yield line

    def seek(self, offset, whence = 0):
        self.subfile.seek(offset, whence)
        RandomFile.seek(self, offset, whence)
        self.contents = self._modify_contents()

# Now, create a base file of uniform (key,number) pairs

file_LCA = RandomFile()

# and now we will create two different sets of modifications for the A and B branches.
#
# To properly exercise the merge, we want some of the changes in A and
# B to be the same on both sides; others will be specific to A or B.
#
# So start each side off with a common set of perturbations

file_common = TweakedRandomFile(file_LCA.clone())

# and now derive the A and B files by two distinct further
# perturbations of that same common set of changes

file_A = TweakedRandomFile(file_common)
file_B = TweakedRandomFile(file_common.clone())

# Now run the merge!  We can rely on the merge auto-dump function to
# dump to ~/.csvmerge3.dump/ if that subdir exists.  So we run here
# with debug disabled; we can rerun the merge from the dump files with
# full debug logging enabled if an error occurs.

csvdiff3.merge3.merge3(file_LCA,
                       file_A,
                       file_B,
                       "name",
                       debug = False)
