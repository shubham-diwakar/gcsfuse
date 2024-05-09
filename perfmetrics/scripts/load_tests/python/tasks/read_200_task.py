"""TODO(ayushsethi): DO NOT SUBMIT without one-line documentation for read_200_task.

TODO(ayushsethi): DO NOT SUBMIT without a detailed description of read_200_task.
"""
import os
import logging
from load_generator import task

MOUNT_DIR = "/mnt/disks/bucket1/"

class OSRead(task.LoadTestTask):
  """Task class for reading file from disk using python's native open api
  """

  def task(self, process_id, thread_id):
    content_len = 0
    for i in range(20):
      file_path = MOUNT_DIR + "Workload.{0}/{1}".format(process_id, i)
      with open(file_path, 'rb') as f_p:
        # read 1M file size
        content = f_p.read(1024*1024)
        content_len = content_len + len(content)
        f_p.close()
    return content_len


