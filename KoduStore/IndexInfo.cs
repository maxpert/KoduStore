using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace KoduStore
{
    internal sealed class IndexInfo
    {
        private readonly IList<MemberInfo> _memberFields;

        private IList<Tuple<MemberInfo, SecondaryIndexAttribute>> _indexMemberList;

        public IndexInfo(Type t)
        {
            this.Type = t;
            _memberFields = this.Type
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Cast<MemberInfo>()
                .Union(this.Type.GetFields(BindingFlags.Instance | BindingFlags.Public))
                .Where(p => p.GetCustomAttribute<SecondaryIndexAttribute>() != null)
                .ToList();

            this.InitializeIndexInfo();
        }

        public Type Type { get; }

        public IDictionary<MemberInfo, IList<SecondaryIndexAttribute>> IndexAttributeMap { get; private set; }

        public IList<Tuple<MemberInfo, SecondaryIndexAttribute>> GetIndexMembersList()
        {
            if (_indexMemberList != null)
            {
                return _indexMemberList;
            }

            var attributesList = new List<Tuple<MemberInfo, SecondaryIndexAttribute>>();
            foreach (var entryAttributes in IndexAttributeMap)
            {
                foreach (var attribute in entryAttributes.Value)
                {
                    attributesList.Add(new Tuple<MemberInfo, SecondaryIndexAttribute>(entryAttributes.Key, attribute));
                }
            }

            _indexMemberList = attributesList;
            return attributesList;
        }

        private void InitializeIndexInfo()
        {
            IndexAttributeMap = new Dictionary<MemberInfo, IList<SecondaryIndexAttribute>>();
            _indexMemberList = null;

            foreach (var member in _memberFields)
            {
                var attibutesList = member.GetCustomAttributes<SecondaryIndexAttribute>()
                    .Select(attribute =>
                    {
                        if (string.IsNullOrEmpty(attribute.Name))
                        {
                            attribute.Name = this.Type.FullName + "." + member.Name;
                        }

                        return attribute;
                    });

                IndexAttributeMap[member] = attibutesList.ToList();
            }
        }
    }
}