using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace RxDatasets.Models
{
    public class ParentsIndex
    {
        private const double OneHour = 1;

        // Top-level uses ConcurrentDictionary keyed by level.
        private readonly ConcurrentDictionary<int, (DateTimeOffset LastRequest, ImmutableDictionary<string, ImmutableHashSet<string>> ParentsWithChildren)> _index
            = new();

        // Semaphore to serialize access to inner immutable updates.
        private readonly SemaphoreSlim _lock = new(1, 1);

        /// <summary>
        /// Retrieves or creates an index of children for a given parent entity at its corresponding level.
        /// If the index for the parent's level does not exist or is requested for the first time,
        /// it scans the provided `theWholeIndex` to build it. Subsequent calls for the same level
        /// will return the cached index, updating its last access time.
        /// </summary>
        /// <param name="parent">The identifier of the parent entity, potentially ending with '/'.</param>
        /// <param name="theWholeIndex">An enumerable collection of all entity names to build the index from if needed.</param>
        /// <returns>
        /// A task that represents the asynchronous operation. The task result contains an
        /// immutable hash set of child entity identifiers for the specified parent.
        /// Returns an empty set if the parent is not found or has no children at that level.
        /// </returns>
        public async Task<ImmutableHashSet<string>> GetOrCreateChildrenIndex(string parent, IEnumerable<string> theWholeIndex)
        {
            using var slot = await _lock.LockAsync();

            parent = parent.TrimEnd('/');
            int requestLevel = parent.Count(c => c == '/') + 1;
            if (!Access(requestLevel, out var existingIndex))
            {
                // Build new inner dictionary by scanning theWholeIndex.
                existingIndex = CreateNewIndex(theWholeIndex, requestLevel);
                var now = DateTimeOffset.UtcNow;
                _index.TryAdd(requestLevel, (now, existingIndex));
            }

            return existingIndex.GetValueOrDefault(parent, ImmutableHashSet<string>.Empty);
        }

        private static ImmutableDictionary<string, ImmutableHashSet<string>> CreateNewIndex(IEnumerable<string> theWholeIndex, int requestLevel)
        {
            var builders = new Dictionary<string, ImmutableHashSet<string>.Builder>();

            foreach (var name in theWholeIndex)
            {
                var foundParent = DatasetHelper.FindParentOfEntity(name, requestLevel);
                if (foundParent is null)
                    continue;

                if (!builders.TryGetValue(foundParent, out var childrenBuilder))
                {
                    childrenBuilder = ImmutableHashSet.CreateBuilder<string>();
                    builders.Add(foundParent, childrenBuilder);
                }

                childrenBuilder.Add(name);
            }

            // Convert builders to immutable collections
            var resultBuilder = ImmutableDictionary.CreateBuilder<string, ImmutableHashSet<string>>();
            foreach (var kvp in builders)
            {
                resultBuilder.Add(kvp.Key, kvp.Value.ToImmutable());
            }

            return resultBuilder.ToImmutable();
        }

        private bool Access(int requestLevel, out ImmutableDictionary<string, ImmutableHashSet<string>> existingIndex)
        {
            if (_index.TryGetValue(requestLevel, out var indexData))
            {
                var now = DateTimeOffset.UtcNow;
                // Update the timestamp and return existing inner dictionary.
                _index[requestLevel] = (now, indexData.ParentsWithChildren);
                existingIndex = indexData.ParentsWithChildren;
                return true;
            }

            existingIndex = ImmutableDictionary<string, ImmutableHashSet<string>>.Empty;
            return false;
        }

        /// <summary>
        /// Cleans the index by removing entries that have not been accessed recently.
        /// This is done by checking the LastRequest timestamp of each entry.
        /// If the entry has not been accessed within the last hour, it is removed.
        /// The method returns true if the index still has data after cleaning, and false otherwise.
        /// </summary>
        /// <returns>
        /// The task result contains a boolean indicating whether the index still has data.
        /// If true, the index still has data; if false, the index is empty.
        /// </returns>
        public async Task<bool> CleanIndex()
        {
            // First, identify which keys need to be removed without holding the lock
            var keysToRemove = _index
                .Where(kvp => !ParentWasRecentlyUsed(kvp.Value.LastRequest))
                .Select(kvp => kvp.Key)
                .ToList();

            // If there's nothing to clean up, check if the index has any data and return
            if (keysToRemove.Count == 0)
                return _index.Count > 0;

            // Only acquire the lock if there's cleanup to do
            using var slot = await _lock.LockAsync();

            // Remove expired entries
            foreach (var key in keysToRemove)
            {
                // Re-check if still expired after acquiring the lock
                if (_index.TryGetValue(key, out var value) && !ParentWasRecentlyUsed(value.LastRequest))
                {
                    _index.TryRemove(key, out _);
                }
            }

            // Check if the index still has data after cleanup
            return _index.Count > 0;
        }

        private static bool ParentWasRecentlyUsed(DateTimeOffset lastRequest) =>
            (DateTimeOffset.UtcNow - lastRequest).TotalHours < OneHour;

        /// <summary>
        /// Updates the index to include a newly added child entity.
        /// It identifies the parent of the child at each existing level in the index
        /// and adds the child to the corresponding parent's children set.
        /// If the parent does not exist at a level, it creates a new entry for the parent with the child.
        /// The last access time for the affected levels is *not* updated by this operation.
        /// </summary>
        /// <param name="child">The identifier of the newly added child entity.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        public async Task OnNew(string child)
        {
            using var slot = await _lock.LockAsync();

            foreach (var level in _index.Keys.ToList())
            {
                var parent = DatasetHelper.FindParentOfEntity(child, level);
                if (parent is null)
                    continue;

                var entry = _index[level];
                var parentsWithChildren = entry.ParentsWithChildren;
                if (parentsWithChildren.TryGetValue(parent, out var children))
                {
                    children = children.Add(child);
                    parentsWithChildren = parentsWithChildren.SetItem(parent, children);
                }
                else
                {
                    parentsWithChildren = parentsWithChildren.Add(parent, ImmutableHashSet<string>.Empty.Add(child));
                }
                _index[level] = (entry.LastRequest, parentsWithChildren);
            }
        }

        /// <summary>
        /// Updates the index to reflect the deletion of a child entity.
        /// It identifies the parent of the child at each existing level in the index
        /// and removes the child from the corresponding parent's children set.
        /// The last access time for the affected levels is *not* updated by this operation.
        /// </summary>
        /// <param name="child">The identifier of the deleted child entity.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        public async Task OnDelete(string child)
        {
            using var slot = await _lock.LockAsync();

            foreach (var level in _index.Keys.ToList())
            {
                var parent = DatasetHelper.FindParentOfEntity(child, level);
                if (parent is null)
                    continue;

                var entry = _index[level];
                var parentsWithChildren = entry.ParentsWithChildren;
                if (parentsWithChildren.TryGetValue(parent, out var children))
                {
                    children = children.Remove(child);
                    parentsWithChildren = parentsWithChildren.SetItem(parent, children);
                    _index[level] = (entry.LastRequest, parentsWithChildren);
                }
            }
        }
    }

}
