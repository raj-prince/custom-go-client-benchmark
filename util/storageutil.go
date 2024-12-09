package util

import (
	"crypto/rand"
	"time"

	"cloud.google.com/go/storage"
	"github.com/jacobsa/gcloud/gcs"
	storagev1 "google.golang.org/api/storage/v1"
)

func convertObjectAccessControlToACLRule(obj *storagev1.ObjectAccessControl) storage.ACLRule {
	aclObj := storage.ACLRule{
		Entity:   storage.ACLEntity(obj.Entity),
		EntityID: obj.EntityId,
		Role:     storage.ACLRole(obj.Role),
		Domain:   obj.Domain,
		Email:    obj.Email,
	}

	if obj.ProjectTeam != nil {
		aclObj.ProjectTeam = &storage.ProjectTeam{
			ProjectNumber: obj.ProjectTeam.ProjectNumber,
			Team:          obj.ProjectTeam.Team,
		}
	}

	return aclObj
}

func SetAttrsInWriter(wc *storage.Writer, req *gcs.CreateObjectRequest) *storage.Writer {
	wc.Name = req.Name
	wc.ContentType = req.ContentType
	wc.ContentLanguage = req.ContentLanguage
	wc.ContentEncoding = req.ContentEncoding
	wc.CacheControl = req.CacheControl
	wc.Metadata = req.Metadata
	wc.ContentDisposition = req.ContentDisposition
	wc.CustomTime, _ = time.Parse(time.RFC3339, req.CustomTime)
	wc.EventBasedHold = req.EventBasedHold
	wc.StorageClass = req.StorageClass

	// Converting []*storagev1.ObjectAccessControl to []ACLRule for writer object.
	var aclRules []storage.ACLRule
	for _, element := range req.Acl {
		aclRules = append(aclRules, convertObjectAccessControlToACLRule(element))
	}
	wc.ACL = aclRules

	if req.CRC32C != nil {
		wc.CRC32C = *req.CRC32C
		wc.SendCRC32C = true
	}

	if req.MD5 != nil {
		wc.MD5 = (*req.MD5)[:]
	}

	return wc
}

func IsStorageConditionsNotEmpty(conditions storage.Conditions) bool {
	return conditions != (storage.Conditions{})
}

func GenerateData(size int) ([]byte, error) {
	data := make([]byte, size*1024*1024)
	if _, err := rand.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}
