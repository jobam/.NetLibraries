using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using Ionic.Zip;
using Microsoft.WindowsAzure;

namespace FileMeDataService
{
    public class FileMeDataManager
    {
        #region Atrributes and Properties

        public string ConnectionString = "UseDevelopmentStorage=true;";
        public const string RootFolderName = "root";
        public const string DefaultFolderName = "default";
        public const string ArchiveFolderName = "archives";
        public const string BackupFolderName = "backups";

        public CloudStorageAccount StorageAccount { get; set; }
        public bool IsPublic { get; set; }
        // blob  a blob client for interacting with the blob service.
        public CloudBlobClient BlobClient { get; set; }
        public CloudBlobContainer Container { get; set; }

        #endregion

        #region Initialize

        public FileMeDataManager(string connectionString = null, bool isPublic = true)
        {
            if (connectionString != null)
                ConnectionString = connectionString;
            IsPublic = isPublic;
            Initialize();
        }

        public async void Initialize()
        {
            StorageAccount = CloudStorageAccount.Parse(ConnectionString);
            BlobClient = StorageAccount.CreateCloudBlobClient();
            Container = BlobClient.GetContainerReference(RootFolderName);
            try
            {
                await Container.CreateIfNotExistsAsync();
                if (IsPublic)
                    await Container.SetPermissionsAsync(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });
            }
            catch (StorageException)
            {
                Debug.WriteLine("If you are running with the dev configuration please make sure you have started the storage emulator.");
                throw;
            }
        }
        #endregion

        #region Actions Methods

        public List<Uri> ListFolders(string directoryName = null)
        {
            List<Uri> foldersNames = new List<Uri>();
            CloudBlobDirectory directory = Container.GetDirectoryReference(directoryName == null ? DefaultFolderName : directoryName);

            var blobs = directory.ListBlobs();
            foreach (var blob in blobs.Where(b => b as CloudBlobDirectory != null))
            {
                foldersNames.Add(blob.Uri);
                Debug.WriteLine(blob.Uri.Segments.Last());
            }
            return foldersNames;
        }

        public List<Uri> ListFiles(string directoryName = null)
        {
            List<Uri> fileNames = new List<Uri>();
            CloudBlobDirectory directory = Container.GetDirectoryReference(directoryName == null ? DefaultFolderName : directoryName);

            foreach (var blob in (directory.ListBlobs()).Where(x => x as CloudBlockBlob != null))
            {
                fileNames.Add(blob.Uri);
                Debug.WriteLine(blob.Uri.Segments.Last());
            }
            return fileNames;
        }

        public async Task<bool> UploadSimpleFile(string filepath, string directoryName = null, string fileDestName = null)
        {
            try
            {
                var filename = fileDestName ?? filepath.Split('\\').Last();
                if (filename.First().Equals('/'))
                    filename = filename.Substring(1, filename.Length - 1);

                CloudBlobDirectory directory = Container.GetDirectoryReference(directoryName == null ? DefaultFolderName : directoryName);
                CloudBlockBlob blockBlob = directory.GetBlockBlobReference(filename);

                await blockBlob.UploadFromFileAsync(filepath, FileMode.Open);
                return true;
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public async Task<bool> UploadSimpleFile(Stream stream, string fileName, string directoryName = null)
        {
            try
            {
                if (fileName.First().Equals('/'))
                    fileName = fileName.Substring(1, fileName.Length - 1);

                CloudBlobDirectory directory = Container.GetDirectoryReference(directoryName == null ? DefaultFolderName : directoryName);
                CloudBlockBlob blockBlob = directory.GetBlockBlobReference(fileName);

                // You have to copy stream to enable seek.
                Stream stream2 = new MemoryStream();
                stream.CopyTo(stream2);
                stream2.Seek(0, 0);
                await blockBlob.UploadFromStreamAsync(stream2);
                return true;
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public bool BackupStorage()
        {
            CloudBlobDirectory destDirectory = Container.GetDirectoryReference(BackupFolderName);
            string directoryName = RootFolderName + ".zip";

            // We use flat blob listening to list blobs of sub folders
            var blobs = Container.ListBlobs(null, true);

            using (ZipFile zipFile = new ZipFile())
            {
                foreach (var file in blobs)
                {
                    Stream currentStream = new MemoryStream();

                    var blob = file as CloudBlockBlob;
                    if (blob != null)
                    {
                        blob.DownloadToStream(currentStream);

                        currentStream.Seek(0, SeekOrigin.Begin);
                        //=========clean path==========
                        var path = blob.Uri.PathAndQuery;
                        path = path.Replace(Container.Uri.AbsolutePath, "");
                        path = path.Replace("%20", " ");
                        //==============================
                        zipFile.AddEntry(path, currentStream);
                    }
                }
                // Upload zip file to Storage
                using (var zipStream = new MemoryStream())
                {
                    zipFile.Save(zipStream);
                    zipStream.Seek(0, SeekOrigin.Begin);

                    var blockBlob = destDirectory.GetBlockBlobReference(directoryName);
                    blockBlob.UploadFromStream(zipStream);
                }
            }
            return true;
        }

        #endregion

        #region Zip

        public bool ZipDirectory(string directoryPath)
        {
            CloudBlobDirectory destDirectory = Container.GetDirectoryReference(ArchiveFolderName);
            string directoryName = directoryPath.Split('/').Last() + ".zip";
            CloudBlobDirectory sourceDir = Container.GetDirectoryReference(directoryPath);

            // we use flat listening to list subdir blobs
            var files = sourceDir.ListBlobs(true);

            using (ZipFile zipFile = new ZipFile())
            {
                foreach (var file in files)
                {
                    Stream currentStream = new MemoryStream();

                    var blob = file as CloudBlockBlob;
                    if (blob != null)
                    {
                        blob.DownloadToStream(currentStream);

                        currentStream.Seek(0, SeekOrigin.Begin);
                        //=========clean path==========
                        var path = blob.Uri.AbsolutePath;
                        path = path.Replace(Container.Uri.AbsolutePath,"");
                        path = path.Replace("%20", " ");
                        //==============================
                        zipFile.AddEntry(path, currentStream);
                    }
                }
                // Upload zip file to Storage
                using (var zipStream = new MemoryStream())
                {
                    zipFile.Save(zipStream);
                    zipStream.Seek(0, SeekOrigin.Begin);

                    var blockBlob = destDirectory.GetBlockBlobReference(directoryName);
                    blockBlob.UploadFromStream(zipStream);
                }
            }
            return true;
        }

        public async Task<bool> UnzipToDirectory(string filePath)
        {
            ZipFile zip = ZipFile.Read(filePath);

            // create directory to blobstorage

            string destDirName = Path.GetFileNameWithoutExtension(filePath.Split('\\').Last());
            try
            {
                foreach (ZipEntry e in zip)
                {
                    using (Stream stream = new MemoryStream())
                    {
                        e.Extract(stream);
                        if (stream.Length != 0)
                        {
                            stream.Seek(0, 0);
                            await UploadSimpleFile(stream, e.FileName, destDirName);
                        }

                    }

                }
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public async Task<bool> UnzipToDirectory(Stream sourceStream, string filename)
        {
            // You have to copy stream to enable seek method
            Stream cleanStream = new MemoryStream();
            sourceStream.CopyTo(cleanStream);
            cleanStream.Seek(0, 0);

            ZipFile zip = ZipFile.Read(cleanStream);

            // create directory to blobstorage

            string destDirName = Path.GetFileNameWithoutExtension(filename);
            try
            {
                foreach (ZipEntry e in zip)
                {
                    using (Stream stream = new MemoryStream())
                    {
                        e.Extract(stream);
                        if (stream.Length != 0)
                        {
                            stream.Seek(0, 0);
                            await UploadSimpleFile(stream, e.FileName, destDirName);
                        }

                    }

                }
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        #endregion
    }
}
